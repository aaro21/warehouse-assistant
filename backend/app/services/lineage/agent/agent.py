

from langchain.agents import initialize_agent, AgentType
from langchain_openai import AzureChatOpenAI
from app.services.lineage.agent.tools import tools, AGENT_SYSTEM_MESSAGE
import os

# tools are imported from tools.py

# Initialize the LangChain agent
llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT", "model-router"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
    temperature=0,
    max_tokens=2048,
)

agent_executor = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    agent_kwargs={"system_message": AGENT_SYSTEM_MESSAGE},
    handle_parsing_errors=True
)


# Utility function to run agent query
def run_agent_query(question: str):
    answer = agent_executor.run(question)
    return {"answer": answer}