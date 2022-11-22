from unittest.mock import Mock

import pytest

from faust.web import Request, View, Web
from faust.web.exceptions import MethodNotAllowed
from tests.helpers import AsyncMock


@View.from_handler
async def foo(self, request):
    return self, request


class Test_View:
    @pytest.fixture
    def web(self):
        return Mock(name="web", autospec=Web)

    @pytest.fixture
    def view(self, *, app, web):
        return foo(app, web)

    def test_from_handler(self):
        assert issubclass(foo, View)

    def test_from_handler__not_callable(self):
        with pytest.raises(TypeError):
            View.from_handler(1)

    def test_init(self, *, app, web, view):
        assert view.app is app
        assert view.web is web
        assert view.methods == {
            "head": view.head,
            "get": view.get,
            "post": view.post,
            "patch": view.patch,
            "delete": view.delete,
            "put": view.put,
            "options": view.options,
            "search": view.search,
        }

    def test_get_methods(self, view):
        assert view.get_methods() == set({"GET", "HEAD"})

    @pytest.mark.parametrize(
        "method",
        [
            "GET",
            "POST",
            "PATCH",
            "DELETE",
            "PUT",
            "OPTIONS",
            "SEARCH",
            "get",
            "post",
            "patch",
            "delete",
            "put",
            "options",
            "search",
        ],
    )
    @pytest.mark.asyncio
    async def test_dispatch(self, method, *, view):
        request = Mock(name="request", autospec=Request)
        request.method = method
        request.match_info = {}
        handler = AsyncMock(name=method)
        view.methods[method.lower()] = handler
        assert await view(request) is handler.return_value
        handler.assert_called_once_with(request)

    def test_path_for(self, *, view):
        ret = view.path_for("name", foo=1)
        assert ret is view.web.url_for.return_value
        view.web.url_for.assert_called_once_with("name", foo=1)

    def test_url_for__no_base(self, *, view):
        view.app.conf.canonical_url = "http://example.com/"
        ret = view.url_for("name", foo=1)
        assert ret

    def test_url_for__base(self, *, view):
        ret = view.url_for("name", "http://example.bar", foo=1)
        assert ret

    @pytest.mark.asyncio
    async def test_get(self, *, view):
        req = Mock(name="request", autospec=Request)
        assert (await view.methods["get"](req)) == (view, req)

    @pytest.mark.asyncio
    async def test_interface_get(self, *, app, web):
        view = View(app, web)
        with pytest.raises(MethodNotAllowed):
            await view.get(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_post(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.post(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_put(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.put(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_patch(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.patch(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_delete(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.delete(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_options(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.options(Mock(name="request", autospec=Request))

    @pytest.mark.asyncio
    async def test_interface_search(self, *, view):
        with pytest.raises(MethodNotAllowed):
            await view.search(Mock(name="request", autospec=Request))

    def test_text(self, *, view, web):
        response = view.text(
            "foo",
            content_type="app/json",
            status=101,
            reason="foo",
            headers={"k": "v"},
        )
        web.text.assert_called_once_with(
            "foo",
            content_type="app/json",
            status=101,
            reason="foo",
            headers={"k": "v"},
        )
        assert response is web.text()

    def test_html(self, *, view, web):
        response = view.html(
            "foo",
            status=101,
            content_type="text/html",
            reason="bar",
            headers={"k": "v"},
        )
        web.html.assert_called_once_with(
            "foo",
            status=101,
            content_type="text/html",
            reason="bar",
            headers={"k": "v"},
        )
        assert response is web.html()

    def test_json(self, *, view, web):
        response = view.json(
            "foo",
            status=101,
            content_type="application/json",
            reason="bar",
            headers={"k": "v"},
        )
        web.json.assert_called_once_with(
            "foo",
            status=101,
            content_type="application/json",
            reason="bar",
            headers={"k": "v"},
        )
        assert response is web.json()

    def test_bytes(self, *, view, web):
        response = view.bytes(
            "foo",
            content_type="app/json",
            status=101,
            reason="foo",
            headers={"k": "v"},
        )
        web.bytes.assert_called_once_with(
            "foo",
            content_type="app/json",
            status=101,
            reason="foo",
            headers={"k": "v"},
        )
        assert response is web.bytes()

    def test_route(self, *, view, web):
        handler = Mock(name="handler")
        res = view.route("pat", handler)
        web.route.assert_called_with("pat", handler)
        assert res is handler

    def test_error(self, *, view, web):
        response = view.error(303, "foo", arg="bharg")
        web.json.assert_called_once_with(
            {"error": "foo", "arg": "bharg"},
            status=303,
            reason=None,
            headers=None,
            content_type=None,
        )
        assert response is web.json()

    def test_notfound(self, *, view, web):
        response = view.notfound(arg="bharg")
        web.json.assert_called_once_with(
            {"error": "Not Found", "arg": "bharg"},
            status=404,
            reason=None,
            headers=None,
            content_type=None,
        )
        assert response is web.json()

    @pytest.mark.asyncio
    async def test_read_request_content(self, *, view, web):
        request = Mock(name="request")
        web.read_request_content = AsyncMock()
        ret = await view.read_request_content(request)
        web.read_request_content.assert_called_once_with(request)
        assert ret is web.read_request_content.return_value
