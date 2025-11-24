
import json
import uuid
from databricks.sdk import WorkspaceClient
from databricks_openai import UCFunctionToolkit, DatabricksFunctionClient
from typing import Any, Optional

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext

# OpenAI-Client abrufen, der für die Kommunikation mit Databricks-Model-Serving-Endpunkten konfiguriert ist
# Diesen verwenden wir, um in unserem Agenten ein LLM abzufragen
openai_client = WorkspaceClient().serving_endpoints.get_open_ai_client()

# Der folgende Codeausschnitt versucht, aus einer Menge von Kandidaten
# die erste in deinem Databricks-Workspace verfügbare LLM-API auszuwählen.
# Man kann ihn überschreiben und vereinfachen, indem man einfach den LLM_ENDPOINT_NAME direkt angibt.
LLM_ENDPOINT_NAME = None

def is_endpoint_available(endpoint_name):
  try:
    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    client.chat.completions.create(model=endpoint_name, messages=[{"role": "user", "content": "What is AI?"}])
    return True
  except Exception:
    return False
  
for candidate_endpoint_name in ["databricks-claude-3-7-sonnet", "databricks-meta-llama-3-3-70b-instruct"]:
    if is_endpoint_available(candidate_endpoint_name):
      LLM_ENDPOINT_NAME = candidate_endpoint_name
assert LLM_ENDPOINT_NAME is not None, "Please specify LLM_ENDPOINT_NAME"

# Logge automatisch Traces aus LLM-Aufrufen, um das Debuggen zu erleichtern
mlflow.openai.autolog()

# Databricks built-in tools laden (ein zustandsloses Python-Code-Interpreter-Tool)
client = DatabricksFunctionClient()
builtin_tools = UCFunctionToolkit(function_names=["system.ai.python_exec"], client=client).tools
for tool in builtin_tools:
    del tool["function"]["strict"]

def call_tool(tool_name, parameters):
    if tool_name == "system__ai__python_exec":
        return DatabricksFunctionClient().execute_function("system.ai.python_exec", parameters=parameters)
    raise ValueError(f"Unknown tool: {tool_name}")

def run_agent(prompt):
    """
    Send a user prompt to the LLM, and return a list of LLM response messages
    The LLM is allowed to call the code interpreter tool if needed, to respond to the user
    """
    result_msgs = []
    response = openai_client.chat.completions.create(
        model=LLM_ENDPOINT_NAME,
        messages=[{"role": "user", "content": prompt}],
        tools=builtin_tools,
    )
    msg = response.choices[0].message
    result_msgs.append(msg.to_dict())

    # Falls das Modell ein Tool ausgeführt hat, rufe es auf
    if msg.tool_calls:
        call = msg.tool_calls[0]
        tool_result = call_tool(call.function.name, json.loads(call.function.arguments))
        result_msgs.append({"role": "tool", "content": tool_result.value, "name": call.function.name, "tool_call_id": call.id})
    return result_msgs

class QuickstartAgent(ChatAgent):
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        prompt = messages[-1].content
        raw_msgs = run_agent(prompt)
        out = []
        for m in raw_msgs:
            out.append(ChatAgentMessage(
                id=uuid.uuid4().hex,
                **m
            ))

        return ChatAgentResponse(messages=out)

AGENT = QuickstartAgent()
mlflow.models.set_model(AGENT)
