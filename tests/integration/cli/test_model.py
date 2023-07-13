class Test_Arena:
    def test_json(self, faust_json):
        exitcode, model, stderr = faust_json("model", "app.Arena")
        assert not exitcode

        assert model == [
            {
                "field": "points",
                "type": "typing.List[__main__.Point]",
                "default": "*",
            },
            {"field": "timestamp", "type": "float", "default": "None"},
        ]

    def test_tabulated(self, faust):
        exitcode, stdout, stderr = faust("model", "app.Arena")
        assert not exitcode
        assert b"typing.List" in stdout


class Test_Point:
    def test_json(self, faust_json):
        exitcode, model, stderr = faust_json("model", "app.Point")
        assert not exitcode

        assert model == [
            {"field": "x", "type": "int", "default": "*"},
            {"field": "y", "type": "int", "default": "*"},
        ]

    def test_tabulated(self, faust):
        exitcode, stdout, stderr = faust("model", "app.Point")
        assert not exitcode
        assert b"int" in stdout
