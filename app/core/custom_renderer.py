from fastapi.responses import JSONResponse
from typing import Any


class CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        response_content = {
            "status": "success",
            "data": content,
            "message": "Request successfully processed."
        }
        return super().render(response_content)


class CustomHTTPException(JSONResponse):
    def render(self, message: Any, data={}) -> bytes:
        response_content = {
            "status": "error",
            "data": data,
            "message": message
        }
        return super().render(response_content)

