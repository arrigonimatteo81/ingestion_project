class QueryContext:
    def __init__(self, **params):
        self.params = params

class QueryRenderer:
    @staticmethod
    def render(template: str, params: dict) -> str:
        query = template
        for k, v in params.items():
            query = query.replace(f"${{{k}}}", str(v))
        return query