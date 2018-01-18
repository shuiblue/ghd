
from __future__ import unicode_literals

import networkx as nx
import pandas as pd

from collections import defaultdict
import logging

from common import decorators as d
from common import mapreduce
import scraper

# ecosystems
import npm
import pypi

ECOSYSTEMS = {
    'npm': npm,
    'pypi': pypi
}

logger = logging.getLogger("ghd")
fs_cache = d.fs_cache('common')

# TODO: deprecate START_DATES
START_DATES = {  # default start dates for ecosystem datasets
    'npm': '2010',
    'pypi': '2005'
}


def get_ecosystem(ecosystem):
    """ Return ecosystem obj if supported, raise ValueError otherwiese """
    if ecosystem not in ECOSYSTEMS:
        raise ValueError(
            "Ecosystem %s is not supported. Only (%s) are supported so far" % (
                ecosystem, ",".join(ECOSYSTEMS.keys())))
    return ECOSYSTEMS[ecosystem]


def package_urls(ecosystem):
    # type: (str) -> pd.DataFrame
    """Get list of packages and their respective GitHub repositories"""
    es = _get_ecosystem(ecosystem)
    return es.packages_info()["url"].dropna()


def package_owners(ecosystem):
    es = _get_ecosystem(ecosystem)
    return es.packages_info()["author"]


@fs_cache
def first_contrib_dates(ecosystem):
    # type: (str) -> pd.Series
    # ~100 without caching
    return pd.Series({package: scraper.commits(url)['authored_date'].min()
                      for package, url in package_urls(ecosystem).iteritems()})


@fs_cache
def monthly_data(ecosystem, metric):
    # type: (str, str) -> pd.DataFrame
    """
    :param ecosystem: str
    :param metric: str:
    :return: pd.DataFrame
    """
    # providers are expected to accept package github url
    # and return a single column dataframe
    assert metric in SUPPORTED_METRICS, "Metric is not supported"
    metric_provider = SUPPORTED_METRICS[metric]

    def gen():
        for package, url in package_urls(ecosystem).iteritems():
            logger.info("Processing %s", package)
            yield metric_provider(url).rename(package)

    return pd.DataFrame(gen())


def contributors(ecosystem, months=1):
    # type: (str) -> pd.DataFrame
    assert months > 0
    """ Get a historical list of developers contributing to ecosystem projects
    This function takes 7m20s for 54k PyPi projects @months=1, 23m20s@4
    :param ecosystem: {"pypi"|"npm"}
    :return: pd.DataFrame, index is projects, columns are months, cells are
        sets of stirng github usernames
    """
    fname = fs_cache.get_cache_fname("contributors", ecosystem, months)
    if fs_cache.expired(fname):
        # fcd = first_contrib_dates(ecosystem).dropna()
        start = scraper.MIN_DATE
        columns = [d.strftime("%Y-%m")
                   for d in pd.date_range(start, 'now', freq="M")][:-3]

        def gen():
            for package, repo in package_urls(ecosystem).items():
                logger.info("Processing %s: %s", package, repo)
                s = scraper.commit_user_stats(repo).reset_index()[
                    ['authored_date', 'author']].groupby('authored_date').agg(
                    lambda df: set(df['author']))['author'].rename(
                    package).reindex(columns)
                if months > 1:
                    s = pd.Series(
                        (set().union(*[c for c in s[max(0, i-months+1):i+1]
                                     if c and pd.notnull(c)])
                         for i in range(len(columns))),
                        index=columns, name=package)
                yield s

        df = pd.DataFrame(gen(), columns=columns)

        # transform and write the dataframe
        df.applymap(
            lambda s: ",".join(str(u) for u in s) if s and pd.notnull(s) else ""
        ).to_csv(fname)

        return df

    df = pd.read_csv(fname, index_col=0, dtype=str)
    return df.applymap(
        lambda s: set(s.split(",")) if s and pd.notnull(s) else set())


@fs_cache
def connectivity(ecosystem, months=1000):
    # type: (str, int) -> pd.DataFrame
    """ Number of projects focal project is connected to via its developers

    :param ecosystem: {"pypi"|"npm"}
    :param months: number of months to lookbehind for shared contributors
    :type ecosystem: str
    :type months: int
    :return: pd.DataFrame, index is projects, columns are months
    :rtype months: pd.DataFrame
    """
    # "-" stands for anonymous user
    cs = contributors(ecosystem, months).applymap(
        lambda x: x.difference(["-"]) if pd.notnull(x) else x)
    owners = package_urls(ecosystem).map(lambda x: x.split("/", 1)[0])

    def gen():
        for month, row in cs.T.iterrows():
            logger.info("Processing %s", month)
            conn = []

            projects = defaultdict(set)
            for project, users in row.items():
                for user in users:
                    projects[user].add(project)

            for project, users in row.items():
                ps = set().union(*[projects[user] for user in users])
                conn.append(sum(owners[p] != owners[project] for p in ps))

            yield pd.Series(conn, index=row.index, name=month)

    return pd.DataFrame(gen(), columns=cs.index).T


@fs_cache
def account_data(ecosystem):
    urls = package_urls(ecosystem)
    users = set(repo_url.split("/", 1)[0].lower() for repo_url in urls)
    api = scraper.GitHubAPI()

    def gen():
        for user in users:
            try:
                yield api.user_info(user)
            except scraper.RepoDoesNotExist:
                continue

    df = pd.DataFrame(
        gen(), columns=['id', 'login', 'org', 'type', 'public_repos',
                        'followers', 'following', 'created_at', 'updated_at'])
    df['org'] = df['type'].map({"Organization": True, "User": False})

    return df.drop('type', 1).set_index('login')


def upstreams(ecosystem):
    # type: (str) -> pd.DataFrame
    # ~12s without caching
    es = get_ecosystem(ecosystem)
    deps = es.dependencies().sort_values("date")
    # will drop 101 record out of 4M for npm
    deps = deps[pd.notnull(deps["date"])]
    deps['deps'] = deps['deps'].map(
        lambda x: set(x.split(",")) if x and pd.notnull(x) else set())

    # pypi was started around 2000, first meaningful numbers around 2005
    # npm was started Jan 2010, first meaningful release 2010-11
    idx = [d.strftime("%Y-%m")
           for d in pd.date_range(deps['date'].min(), 'now', freq="M")]

    # for several releases per month, use the last value
    df = deps.groupby([deps.index, deps['date'].str[:7]])['deps'].last()
    return df.unstack(level=-1).T.reindex(idx).fillna(method='ffill').T


def downstreams(uss):
    """ Basically, reversed upstreams
    :param uss: either ecosystem (pypi|npm) or an upstreams DataFrame
    :return: pd.DataFrame, df.loc[project, month] = set([*projects])
    """
    # ~35s without caching
    if isinstance(uss, str):
        uss = upstreams(uss)

    def gen(row):
        s = defaultdict(set)
        for pkg, dss in row.items():
            if dss and pd.notnull(dss):
                # add package as downstream to each of upstreams
                for ds in dss:
                    s[ds].add(pkg)
        return pd.Series(s, name=row.name, index=row.index)

    return uss.apply(gen, axis=0)


def cumulative_dependencies(deps):
    # apply - 150s
    # owners = package_owners("pypi")

    def gen(dependencies):
        cumulative_upstreams = {}

        def traverse(pkg):
            if pkg not in cumulative_upstreams:
                cumulative_upstreams[pkg] = set()  # prevent infinite loop
                ds = dependencies[pkg]
                if ds and pd.notnull(ds):
                    cumulative_upstreams[pkg] = set.union(
                        ds, *(traverse(d) for d in ds if d in dependencies))
            return cumulative_upstreams[pkg]

        return pd.Series(dependencies.index, index=dependencies.index).map(
            traverse).rename(dependencies.name)

    return deps.apply(gen, axis=0)


def count_values(df):
    # type: (pd.DataFrame) -> pd.DataFrame
    """ Count number of values in lists/sets
    It is initially introduced to count dependencies
    """
    # takes around 20s for full pypi history

    def count(s):
        return len(s) if s and pd.notnull(s) else 0

    if isinstance(df, pd.DataFrame):
        return df.applymap(count)
    elif isinstance(df, pd.Series):
        return df.apply(count)


def centrality(how, graph, *args, **kwargs):
    # type: (str, nx.Graph) -> dict
    if not how.endswith("_centrality") and how not in \
            ('communicability', 'communicability_exp', 'estrada_index',
             'communicability_centrality_exp', "subgraph_centrality_exp",
             'dispersion', 'betweenness_centrality_subset', 'edge_load'):
        how += "_centrality"
    assert hasattr(nx, how), "Unknown centrality measure: " + how
    return getattr(nx, how)(graph, *args, **kwargs)


@fs_cache
def dependencies_centrality(ecosystem, start_date, centrality_type):
    """
    [edge_]current_flow_closeness is not defined for digraphs
    current_flow_betweenness - didn't try
    communicability*
    estrada_index
    """
    uss = upstreams(ecosystem).loc[:, start_date:]

    def gen(stub):
        # stub = uss column
        logger.info("Processing %s", stub.name)
        g = nx.DiGraph()
        for pkg, us in stub.items():
            if not us or pd.isnull(us):
                continue
            for u in us:  # u is upstream name
                g.add_edge(pkg, u)

        return pd.Series(centrality(centrality_type, g), index=stub.index)

    return uss.apply(gen, axis=0).fillna(0)


@fs_cache
def contributors_centrality(ecosystem, centrality_type, months, *args):
    """
    {in|out}_degree are not supported
    eigenvector|katz - didn't converge (increase number of iterations?)
    current_flow_* - requires connected graph
    betweenness_subset* - requires sources
    communicability - doesn't work, internal error
    subgraph - unknown (update nx?)
    local_reaching - requires v
    """
    contras = contributors(ecosystem, months)
    # {in|out}_degree is not defined for undirected graphs

    def gen(stub):
        logger.info("Processing %s", stub.name)
        projects = defaultdict(set)  # projects[contributor] = set(projects)

        for pkg, cs in stub.iteritems():
            if not cs or pd.isnull(cs):
                continue
            for c in cs:
                projects[c].add(pkg)

        projects["-"] = set()
        g = nx.Graph()

        for pkg, cs in stub.iteritems():
            for c in cs:
                for p in projects[c]:
                    if p != pkg:
                        g.add_edge(pkg, p)
        return pd.Series(centrality(centrality_type, g, *args),
                         index=stub.index)

    return contras.apply(gen, axis=0).fillna(0)


def slice(project_name, url):
    cs = scraper.commit_stats(url)
    if not len(cs):  # repo does not exist
        return None

    df = pd.DataFrame({
        'age': range(len(cs)),
        'project': project_name,
        'dead': None,
        'last_observation': 0,
        'commercial': scraper.commercial_involvement(url).reindex(
            cs.index, fill_value=0),
        'university': scraper.university_involvement(url).reindex(
            cs.index, fill_value=0),
        'org': False,  # FIXME
        'license': None,  # FIXME
        'commits': cs,
        'q50': scraper.contributions_quantile(url, 0.5).reindex(
            cs.index, fill_value=0),
        'q70': scraper.contributions_quantile(url, 0.7).reindex(
            cs.index, fill_value=0),
        'q90': scraper.contributions_quantile(url, 0.9).reindex(
            cs.index, fill_value=0),
        'gini': scraper.commit_gini(url).reindex(
            cs.index),
        'issues': scraper.new_issues(url).reindex(
            cs.index, fill_value=0),
        'non_dev_issues': scraper.non_dev_issue_stats(url).reindex(
            cs.index, fill_value=0),
        'submitters': scraper.submitters(url).reindex(
            cs.index, fill_value=0),
        'non_dev_submitters': scraper.non_dev_submitters(url).reindex(
            cs.index, fill_value=0),
        'downstreams': None,  # FIXME
        'upstreams': None,  # FIXME
        't_downstreams': None,
        't_upstreams': None,
        'cc_X': None,
        'dc_X': None
    })

    # FIXME: df = pd.rolling_mean(window=smoothing, center=False)
    # FIXME: set last_observation iloc[-1] to 1

    return df


def survival_data(ecosystem, smoothing=1):
    """
    :param ecosystem: ("npm"|"pypi")
    :param smoothing:  number of month to average over
    :return: pd.Dataframe with columns:
         age, date, project, dead, last_observation
         commercial, university, org, license,
         commits, contributors, q50, q70, q90, gini,
         issues, non_dev_issues, submitters, non_dev_submitters
         downstreams, upstreams, transitive downstreams, transitive upstreams,
         contributors centrality,
         dependencies centrality
    """
    # es = get_ecosystem(ecosystem)
    log = logging.getLogger("ghd.common.survival_data")

    def gen():
        for project_name, url in package_urls(ecosystem).items():
            log.info(url)
            df = slice(project_name, url)
            for _, row in df:
                yield row

    return pd.DataFrame(gen()).reset_index(drop=True)
