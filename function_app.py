
print ("\n")
# print("\U0001F6E0  " * 10)
print("= * " * 10)
print ("vida rail SMT chatbot")
print("* = " * 10)
# print("\U0001F6E0  " * 10)
print ("\n")


import azure.functions as func
import logging

from smt_ai_core import smt_chatbot_request

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# @app.route(route="http_trigger")
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     name = req.params.get('name')
#     if not name:
#         try:
#             req_body = req.get_json()
#         except ValueError:
#             pass
#         else:
#             name = req_body.get('name')

#     if name:
#         return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
#     else:
#         return func.HttpResponse(
#              "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
#              status_code=200
#         )



@app.route(route="chatbot_request")
def chatbot_request(req: func.HttpRequest) -> func.HttpResponse:

    request_json = req.get_json()

    ChatbotAiMessageId_output = smt_chatbot_request (request_json)

    ChatbotAiMessageId_output = str (ChatbotAiMessageId_output)

    return func.HttpResponse( ChatbotAiMessageId_output , status_code=200)