"""Which built-in blueprints get served, for each combination of flags.

Before these settings existed the choice was driven entirely by
:setting:`debug`: ``/router`` and ``/table`` were always on, and ``debug``
swapped the production index for the statistics and graph endpoints.  The
back-compat cases below pin that exact behaviour for apps that set none of
the new settings.
"""

import pytest

import faust

ROUTER = "faust.web.apps.router:blueprint"
TABLES = "faust.web.apps.tables.blueprint"
GRAPH = "faust.web.apps.graph:blueprint"
STATS = "faust.web.apps.stats:blueprint"
METRICS = "faust.web.apps.metrics:blueprint"
INDEX = "faust.web.apps.production_index:blueprint"


def enabled(**settings):
    app = faust.App("t-flags", store="memory://", cache="memory://", **settings)
    return {bp for _prefix, bp in app.web._enabled_blueprints()}


class Test_backwards_compatibility:
    def test_defaults_match_the_old_non_debug_behaviour(self):
        assert enabled() == {ROUTER, TABLES, INDEX}

    def test_debug_matches_the_old_debug_behaviour(self):
        assert enabled(debug=True) == {ROUTER, TABLES, GRAPH, STATS}

    def test_metrics_is_off_by_default(self):
        assert METRICS not in enabled()
        assert METRICS not in enabled(debug=True)


class Test_flags:
    @pytest.mark.parametrize(
        "setting,blueprint",
        [
            ("web_router_enabled", ROUTER),
            ("web_tables_enabled", TABLES),
        ],
    )
    def test_on_by_default_and_can_be_turned_off(self, setting, blueprint):
        assert blueprint in enabled()
        assert blueprint not in enabled(**{setting: False})

    @pytest.mark.parametrize(
        "setting,blueprint",
        [
            ("web_graph_enabled", GRAPH),
            ("web_stats_enabled", STATS),
            ("web_metrics_enabled", METRICS),
        ],
    )
    def test_off_by_default_and_can_be_turned_on(self, setting, blueprint):
        assert blueprint not in enabled()
        assert blueprint in enabled(**{setting: True})

    def test_graph_and_stats_are_independent(self):
        """The point of the change: debug used to enable both or neither."""
        only_graph = enabled(debug=True, web_stats_enabled=False)
        assert GRAPH in only_graph
        assert STATS not in only_graph

        only_stats = enabled(debug=True, web_graph_enabled=False)
        assert STATS in only_stats
        assert GRAPH not in only_stats

    def test_router_and_tables_can_be_locked_down(self):
        """Neither could be disabled at all before these settings."""
        assert enabled(web_router_enabled=False, web_tables_enabled=False) == {INDEX}


class Test_root_path:
    """Stats and the production index both mount at "/" -- exactly one wins."""

    def test_index_serves_root_when_stats_is_off(self):
        served = enabled(web_stats_enabled=False)
        assert INDEX in served
        assert STATS not in served

    def test_stats_replaces_the_index(self):
        served = enabled(web_stats_enabled=True)
        assert STATS in served
        assert INDEX not in served

    def test_root_is_never_unserved(self):
        for settings in ({}, {"debug": True}, {"web_stats_enabled": True}):
            served = enabled(**settings)
            assert (STATS in served) != (INDEX in served)


class Test_unknown_blueprints:
    def test_blueprints_without_a_flag_are_always_enabled(self):
        """A subclass adding blueprints must not need to register a flag."""
        app = faust.App("t-extra", store="memory://", cache="memory://")
        web = app.web
        web.optional_blueprints = [("/custom", "proj.web:blueprint")]

        assert "proj.web:blueprint" in {bp for _p, bp in web._enabled_blueprints()}
