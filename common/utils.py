
from __future__ import unicode_literals

import networkx as nx
import pandas as pd

from collections import defaultdict
import logging
import re

from common import decorators as d
from common import mapreduce
from common import versions
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

""" This lookup is used by parse_license()
Since many license strings contain several (often conflicting) licenses,
the least restrictive license takes precedence.

https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
"""

LICENSE_TYPES = (
    (  # permissive
        ('apache', 'Apache'),
        ('isc', 'ISC'),
        ('mit', 'MIT'),
        ('bsd', 'BSD'),
        ('wtf', 'WTFPL'),
        ('public', 'PD'),
        ('unlicense', 'PD'),
    ),
    (  # somewhat restrictive
        ('mozilla', 'MPL'),
        # 'mpl' will also match words like 'simple' and 'example'
    ),
    (  # somewhat permissive
        ('lesser', 'LGPL'),
        ('lgpl', 'LGPL'),
    ),
    (  # strong copyleft
        ('general public', 'GPL'),
        ('gpl', 'GPL'),
        ('affero', 'GPL'),
        ('CC-BY-SA', 'CC-BY-SA'),
    ),
    (  # permissive again
        ('CC-BY', 'CC'),
        ('creative', 'CC'),
    ),
)


def get_ecosystem(ecosystem):
    """ Return ecosystem obj if supported, raise ValueError otherwiese """
    if ecosystem not in ECOSYSTEMS:
        raise ValueError(
            "Ecosystem %s is not supported. Only (%s) are supported so far" % (
                ecosystem, ",".join(ECOSYSTEMS.keys())))
    return ECOSYSTEMS[ecosystem]


@fs_cache
def package_urls(ecosystem):
    # type: (str) -> pd.DataFrame
    """ A shortcut to get list of packages having identified repo URL
    Though it looks trivial, it is a rather important method.
    """
    es = get_ecosystem(ecosystem)

    urls = es.packages_info()["url"]
    # this is necessary to get rid of false URLS, such as:
    # - meta-urls, e.g.
    #       http://github.com/npm/deprecate-holder.git
    #       http://github.com/npm/security-holder.git
    # - foundries, i.e. repositories hosting swarms of packages at once
    #       github.com/micropython/micropython-lib
    #       https://bitbucket.org/ronaldoussoren/pyobjc/src
    # - false records generated by code generators
    #       "This project was generated with angular-cli"
    #       "This project was bootstrapped with [Create React App]"
    #       github.com/swagger-api/swagger-codegen
    # NPM: 446K -> 389K
    # PyPI: 91728 -> 86892
    urls = urls[urls.map(urls.value_counts()) == 1]

    def supported_and_exist(project_name, url):
        logger.info(project_name)
        try:
            provider, project_url = scraper.get_provider(url)
        except NotImplementedError:
            return False
        return provider.project_exists(project_url)

    # more than 16 threads make GitHub to choke even on public urls
    se = mapreduce.map(urls, supported_and_exist, num_workers=16)

    return urls[se]


def get_repo_username(url):
    """ This function is used by user_info to extract name of repository owner
    from its URL. It works so far, but violates abstraction and should be
    refactored at some point """
    provider_name, project_url = scraper.parse_url(url)
    # Assuming urls come from package_urls,
    # we already know the provider is supported
    return project_url.split("/", 1)[0]


@fs_cache
def user_info(ecosystem):
    """ Return user profile fields
    Originally this method was created to differentiate org from user accounts

    :param ecosystem: {npm|pypi}
    :return: pd.DataFrame with a bunch of user profile fields (exact set of
            fields depends on repository providers being used)
    """

    def get_user_info(url, row):
        # single column dataframe is used instead of series to simplify
        # result type conversion
        username = row["username"]
        logger.info("Processing %s", username)
        fields = ['created_at', 'login', 'type', 'public_repos',
                  'followers', 'following']
        provider, _ = scraper.get_provider(url)
        try:
            data = provider.user_info(username)
        except scraper.RepoDoesNotExist:
            return {}
        return {field: data.get(field) for field in fields}

    # Since we're going to get many fields out of one, to simplify type
    # conversion it makes sense to convert to pd.DataFrame.
    # by the same reason, user_info() above gets row and not url value
    urls = package_urls(ecosystem)
    urls.index = urls
    # now usernames have
    usernames = urls.map(get_repo_username).rename("username").sort_values()
    usernames = usernames[~usernames.duplicated(keep='first')]
    # GitHub seems to ban IP (will get HTTP 403) if use >8 workers
    ui = mapreduce.map(pd.DataFrame(usernames), get_user_info, num_workers=8)
    ui["org"] = ui["type"].map(lambda x: x == "Organization")
    return ui.drop(["type"], axis=1)


def parse_license(license):
    """ Map raw license string to either a feature, either a class or a numeric
    measure, like openness.
    ~1 second for NPM, no need to cache
    - 3295 unique values in PyPI (lowercase for normalization)
    + gpl + general public - lgpl - lesser = 575 + 152 - 152 + 45 = 530
        includes affero
    + bsd: 358
    + mit: 320
    + lgpl + lesser = 152 + 45 = 197
    + apache: 166
    + creative: 44
    + domain: 34
    + mpl - simpl - mple = 29
    + zpl: 26
    + wtf: 22
    + zlib: 7
    - isc: just a few, but MANY in NPM
    - copyright: 763
        "copyright" is often (50/50) used with "mit"
    """
    if license and pd.notnull(license):
        license = license.lower()
        # the most permissive ones come first
        for license_types in LICENSE_TYPES:
            for token, license_type in license_types:
                if token in license:
                    return license_type
    return None


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


def upstreams(ecosystem):
    # type: (str) -> pd.DataFrame
    # ~66s for pypi, doesn't make sense to cache

    def gen():
        es = get_ecosystem(ecosystem)
        deps = es.dependencies().sort_values("date")
        # will drop 101 record out of 4M for npm
        deps = deps[pd.notnull(deps["date"])]
        deps['deps'] = deps['deps'].map(
            lambda x: set(x.split(",")) if x and pd.notnull(x) else set())

        # for several releases per month, use the last value
        df = deps.groupby([deps.index, deps['date'].str[:7].rename('month')]
                          ).last().reset_index().sort_values(["name", "month"])

        last_release = ""
        last_package = ""
        for _, row in df.iterrows():
            if row["name"] != last_package:
                last_release = ""
                last_package = row["name"]
            if not re.match("^\d+(\.\d+)*$", row["version"]):
                continue
            if versions.compare(row["version"], last_release) < 0:
                continue
            last_release = row["version"]
            yield row

    df = pd.DataFrame(gen(), columns=["name", "month", "deps"])

    # pypi was started around 2000, first meaningful numbers around 2005
    # npm was started Jan 2010, first meaningful release 2010-11
    # no need to cut off anything
    idx = [d.strftime("%Y-%m")
           for d in pd.date_range(df['month'].min(), 'now', freq="M")]

    deps = df.set_index(["name", "month"], drop=True)["deps"]
    # ffill can be dan with axis=1; Transpose here is to reindex
    return deps.unstack(level=0).reindex(idx).fillna(method='ffill').T


def downstreams(uss):
    """ Basically, reversed upstreams
    :param uss: either ecosystem (pypi|npm) or an upstreams DataFrame
    :return: pd.DataFrame, df.loc[project, month] = set([*projects])
    """
    # ~25s on PyPI if uss is an upstreams dataframe
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
    """
   ~160 seconds for pypi upstreams, ?? for downstreams
   Tests:
         A      B
       /  \
      C    D
    /  \
   E    F
   >>> down = pd.DataFrame({
        1: [set(['c', 'd']), set(), set(['e', 'f']), set(), set(), set()]},
            index=['a', 'b', 'c', 'd', 'e', 'f'])
   >>> len(common.cumulative_dependencies(down).loc['a', 1])
   5
   >>> len(common.cumulative_dependencies(down).loc['c', 1])
   2
   >>> len(common.cumulative_dependencies(down).loc['b', 1])
   0
   """

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
    log = logging.getLogger("ghd.common.survival_data")

    def gen():
        es = get_ecosystem(ecosystem)
        log.info("Getting package info and user profiles..")
        ui = user_info(ecosystem)
        pkginfo = es.packages_info()

        log.info("Dependencies counts..")
        uss = upstreams(ecosystem)  # upstreams, every cell is a set()
        dss = downstreams(uss)  # downstreams, every cell is a set()
        usc = count_values(uss)  # upstream counts
        dsc = count_values(dss)  # downstream counts
        # transitive counts
        t_usc = count_values(cumulative_dependencies(uss))
        t_dsc = count_values(cumulative_dependencies(dss))

        log.info("Dependencies centrality..")


        for project_name, url in package_urls(ecosystem).items():
            log.info(url)

            cs = scraper.commit_stats(url)
            if not len(cs):  # repo does not exist
                continue

            df = pd.DataFrame({
                'age': range(len(cs)),
                'project': project_name,
                'dead': None,
                'last_observation': False,
                'org': ui.loc[url, "org"],
                'license': parse_license(pkginfo[project_name, "license"]),
                'commercial': scraper.commercial_involvement(url).reindex(
                    cs.index, fill_value=0),
                'university': scraper.university_involvement(url).reindex(
                    cs.index, fill_value=0),
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
                'upstreams': usc.loc[project_name, cs.index],
                'downstreams': dsc.loc[project_name, cs.index],
                't_downstreams': t_dsc.loc[project_name, cs.index],
                't_upstreams': t_usc.loc[project_name, cs.index],
            })

            'cc_X': None,
            'dc_X': None

            # FIXME: df = pd.rolling_mean(window=smoothing, center=False)
            # FIXME: set last_observation iloc[-1] to 1

            for _, row in df:
                yield row

    return pd.DataFrame(gen()).reset_index(drop=True)
