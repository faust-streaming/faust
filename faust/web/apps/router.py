"""HTTP endpoint showing partition routing destinations."""
from faust import web
from faust.web.exceptions import ServiceUnavailable

__all__ = ["TableList", "TopicList", "TableDetail", "TableKeyDetail", "blueprint"]

blueprint = web.Blueprint("router")


@blueprint.route("/", name="list")
class TableList(web.View):
    """List routes for all tables."""

    async def get(self, request: web.Request) -> web.Response:
        """Return JSON response with list of all table routes."""
        router = self.app.router
        return self.json(router.tables_metadata())


@blueprint.route("/topics", name="list")
class TopicList(web.View):
    """List routes for all external topics."""

    async def get(self, request: web.Request) -> web.Response:
        """Return JSON response with list of all table routes."""
        router = self.app.router
        return self.json(router.external_topics_metadata())


@blueprint.route("/{name}/", name="detail")
class TableDetail(web.View):
    """List route for specific table."""

    async def get(self, request: web.Request, name: str) -> web.Response:
        """Return JSON response with table metadata."""
        router = self.app.router
        return self.json(router.table_metadata(name))


@blueprint.route("/{name}/{key}/", name="key-detail")
class TableKeyDetail(web.View):
    """List information about key."""

    async def get(self, request: web.Request, name: str, key: str) -> web.Response:
        """Return JSON response after looking up the route of a table key.

        Arguments:
            name: Name of table.
            key: Key to look up node for.

        Raises:
            faust.web.exceptions.ServiceUnavailable: if broker metadata
                has not yet been received.
        """
        router = self.app.router
        try:
            dest_url = router.key_store(name, key)
        except KeyError:
            raise ServiceUnavailable()
        else:
            return self.json(str(dest_url))
