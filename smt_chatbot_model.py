
# %%
from dotenv import load_dotenv, find_dotenv
from langchain.agents import create_agent
from langchain_openai import AzureChatOpenAI
from langchain.agents import create_agent
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage

import os

from sqlalchemy.engine import URL
# from langchain.chains import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit


from IPython.display import Markdown, display


from langchain.tools import tool
# import matplotlib.pyplot as plt
import io
import base64
from IPython.display import Image, display
import pandas as pd

# from langchain_experimental.tools.python.tool import PythonREPLTool

import json
import pyodbc
from langgraph.checkpoint.base import BaseCheckpointSaver

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    CheckpointTuple,
)

from langgraph.checkpoint.memory import InMemorySaver  


# %%
_ = load_dotenv(find_dotenv())

# %%
connection_url = URL.create(
    "mssql+pyodbc",
    username = os.environ["AZURE_SS_USER"],
    password = os.environ["AZURE_SS_PWD"],
    host     = os.environ["AZURE_SS_SERVER"],
    database = os.environ["AZURE_SS_DB"],
    query={"driver": "ODBC Driver 17 for SQL Server"}
)

llm = AzureChatOpenAI(
    openai_api_version = os.environ["AZURE_OPENAI_API_VERSION"],
    azure_endpoint     = os.environ["AZURE_OPENAI_ENDPOINT"],
    api_key          = os.environ["AZURE_OPENAI_KEY"],
    # model            = os.environ["AZURE_OPENAI_MODEL"],
    # azure_deployment = os.environ["AZURE_OPENAI_MODEL"],
    azure_deployment = "gpt-5.1-chat",
)

# deployment_name = "gpt-5.1-chat"

db = SQLDatabase.from_uri( connection_url )

# %%
@tool("provide_expenditure_query", return_direct=False)
def provide_expenditure_query():
    """this tool provides the SQL query stetement when the EXPENDITURE by YEAR is requested

    THIS TOOLS DEOSN'T RUN ANY QUERY, just ouptut the query example,
    THE AGENT MUST ADAPT this query, and run it to get the results
    """

    out_val = f"""
```sql

    SELECT Year(fi.month_reference) AS expenditure_year,
           Sum(fi.value) AS expenditure_value
      FROM [dbo].[ecosys_main_export_nonfinancial] nfi
INNER JOIN [dbo].[ecosys_main_export_financial] fi
        ON fi.indexid = nfi.indexid

-- OPTIONAL FILTERS SECTION
/*
WHERE [Solution (Name)] IN ('Rail Over Road','Road Over River','VPS')
AND [Program Alliance (Name)] IN ('SPA')
*/

  GROUP BY Year(fi.month_reference)
  ORDER BY Year(fi.month_reference) ASC

```
"""
    return out_val


@tool("provide_steady_state_query", return_direct=False)
def provide_steady_state_query():
    """this tool provides the SQL query stetement when the STEADY STATE data is required

    THIS TOOLS DEOSN'T RUN ANY QUERY, just ouptut the query example,
    THE AGENT MUST ADAPT this query, and run it to get the results
    the @peak_year variable needs to be adjusted as per the user prompt.

    The CTE escalation_tb should remain intact and
    ```SQL
    EscalationVersionId = 'E2B76BE2-5BEB-FD5C-DF64-CCE33153BC6C'
    ```
    should remain hard coded.
    """

    out_val = f"""
```sql
DECLARE @peak_year INT = 2022

;WITH
base_q as(

    SELECT Year(fi.month_reference) AS expenditure_year,
           Sum(fi.value) AS expenditure_value
      FROM [dbo].[ecosys_main_export_nonfinancial] nfi
INNER JOIN [dbo].[ecosys_main_export_financial] fi
        ON fi.indexid = nfi.indexid

-- OPTIONAL FILTERS SECTION
/*
WHERE [Solution (Name)] IN ('Rail Over Road','Road Over River','VPS')
AND [Program Alliance (Name)] IN ('SPA')
*/

  GROUP BY Year(fi.month_reference)
)
,escalation_tb as (
  select max(EscalationMultiplier) as EscalationMultiplier,
         year(EscalationTargetDate) as EscalationYear
    from [dbo].[Escalation]
   where EscalationVersionId = 'E2B76BE2-5BEB-FD5C-DF64-CCE33153BC6C'
group by year(EscalationTargetDate) 
)

select 
base_q.*,

/*  escalation_tb.EscalationMultiplier,  */ 

case
when expenditure_year < @peak_year then null
when expenditure_year = @peak_year then (select expenditure_value from base_q where expenditure_year = @peak_year)
when escalation_tb.EscalationMultiplier is null then (select expenditure_value from base_q where expenditure_year = @peak_year)
else escalation_tb.EscalationMultiplier * (select expenditure_value from base_q where expenditure_year = @peak_year)
end as steady_state_curve

from base_q
left join escalation_tb on base_q.expenditure_year = escalation_tb.EscalationYear

order by expenditure_year ASC
```
"""
    return out_val


# %%
import tempfile
import matplotlib.pyplot as plt
import json
from langchain.tools import tool
from langchain_openai import ChatOpenAI

llm_codegen = llm

@tool("chart9", return_direct=False)
def chart9(prompt: str) -> str:
    """
    Flexible chart generator using Matplotlib.
    Input: natural-language description + data.
    
    The prompt must contain:
    - what chart to draw
    - data (any format: dict, JSON, CSV-like)
    
    Returns: path to generated PNG file.
    """

    # 1. Ask LLM to create Python/Matplotlib code from the prompt
    code_prompt = f"""
You are a Python Matplotlib generator.

User prompt:
{prompt}

Requirements:
- Generate valid Python code using only matplotlib and json.
- Do NOT include backticks.
- The code must:

    1) Load the provided data (it may be JSON inside the prompt, detect it)
    2) Build the chart requested by the user
    3) Disable scientific notation and axis scale factors using:
         plt.ticklabel_format(style='plain', useOffset=False)
    4) Save to a variable named output_file
    5) Call plt.savefig(output_file)

- Do NOT show the chart interactively.
- Support unlimited data series.
- No hard-coded chart types. Create whatever the user describes.

- guidelines how to format tick labels for axis, series or dimensions

if the values are YEAR present it as a integer without thousands separator, like: 1990, 2010, 2222

if the average value of the axis is a large number order of magniutude of million or more, use the "format in millions" example shown below

- instructions to customize tick formatter:
         
the function below is an example of a formatting function that "format in millions", elaborate another formatting function if needed
    def format_in_millions(x, pos):
        return f"{{int(x/1e6):,}}M"
          
Then apply it (this example apply it in x axis):
    import matplotlib.ticker as mticker
    ax = plt.gca()
    ax.yaxis.set_major_formatter(mticker.FuncFormatter( format_in_millions ) )

- Only return code. No explanation.
"""

    code = llm_codegen.invoke(code_prompt).content

    # 2. Execute the code safely
    temp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    output_file = temp_file.name

    # Create restricted globals
    safe_globals = {
        "plt": plt,
        "json": json,
        "output_file": output_file
    }

    try:
        exec(code, safe_globals)
    except Exception as e:
        return f"Chart generation failed: {e}\nCode was:\n{code}"

    return output_file


# %%
import langchain
print(langchain.__version__)


# %%
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_core.runnables.history import RunnableWithMessageHistory

# %%

class SQLServerSaver(BaseCheckpointSaver):
    def __init__(self, conn_str: str, table: str = "LangGraphCheckpoints"):
        self.conn_str = conn_str
        self.table = table

    def _connect(self):
        return pyodbc.connect(self.conn_str)

    # ----------------------------------------
    # REQUIRED: get_tuple
    # ----------------------------------------
    def get_tuple(self, config):
        thread_id = config["configurable"].get("thread_id")
        checkpoint_id = config["configurable"].get("checkpoint_id")

        if checkpoint_id is None:
            # get newest checkpoint for this thread
            sql = f"""
                SELECT TOP 1 checkpoint_id, checkpoint_data, metadata
                FROM {self.table}
                WHERE thread_id = ?
                ORDER BY checkpoint_id DESC
            """
            params = (thread_id,)
        else:
            sql = f"""
                SELECT checkpoint_id, checkpoint_data, metadata
                FROM {self.table}
                WHERE thread_id = ? AND checkpoint_id = ?
            """
            params = (thread_id, checkpoint_id)

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return None

        # Build CheckpointTuple
        return CheckpointTuple(
            config=config,
            checkpoint=json.loads(row[1]) if row[1] else {},
            metadata=json.loads(row[2]) if row[2] else {},
            parent_config=None,
        )

    # ----------------------------------------
    # REQUIRED: put
    # ----------------------------------------
    def put(self, config, checkpoint, metadata):
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = metadata.get("checkpoint_id")

        conn = self._connect()
        cursor = conn.cursor()

        sql = f"""
        MERGE {self.table} AS tgt
        USING (SELECT ? AS thread_id, ? AS checkpoint_id) AS src
        ON tgt.thread_id = src.thread_id AND tgt.checkpoint_id = src.checkpoint_id
        WHEN MATCHED THEN UPDATE
            SET checkpoint_data = ?, metadata = ?
        WHEN NOT MATCHED THEN
            INSERT (thread_id, checkpoint_id, checkpoint_data, metadata)
            VALUES (?, ?, ?, ?);
        """

        c_json = json.dumps(checkpoint)
        m_json = json.dumps(metadata)

        cursor.execute(
            sql,
            thread_id,
            checkpoint_id,
            c_json,
            m_json,
            thread_id,
            checkpoint_id,
            c_json,
            m_json,
        )

        conn.commit()
        cursor.close()
        conn.close()

        # IMPORTANT: must return a CheckpointTuple
        return CheckpointTuple(
            config=config,
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=None,
        )


# %%
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
sql_tools = toolkit.get_tools()

all_tools = sql_tools + [provide_expenditure_query, provide_steady_state_query,chart9]

agent = create_agent (
    model = llm,
    tools = all_tools
)



agent_with_memory = create_agent (
    model = llm,
    tools = all_tools,
    checkpointer=InMemorySaver()
)


# checkpointer = SQLServerSaver(connection_url)

# agent_with_memory = create_agent (
#     model = llm,
#     tools = all_tools,
#     checkpointer = checkpointer
# )


# [get_user_info]

# %%
graph = agent.get_graph()
from IPython.display import Image
Image(graph.draw_mermaid_png())

# %%
def ai_pretty_print(r):

    display(Markdown( r.get("messages")[-1].content ))
    print ( (" #" * 30 + "\n") * 5  )
    

    r_extract = r.get("messages")
    r_extract = list(reversed(r_extract))

    for  m in r_extract[1:]:
        print ("\n\n",m.content)
        if isinstance(m, HumanMessage):
            line = " &" * 20
        elif isinstance(m, SystemMessage):
            line = " *" * 20 
        elif isinstance(m, AIMessage):
            line = " ^" * 20
        elif isinstance(m, ToolMessage):
            line = " +" * 20   
        else:
            line = " -" * 20
        
        print (line)

# %%
system_prompt = "You are a helpful assistant."

system_prompt = """You are a helpful assistant.

Provide answers in markdown

Tables and Columns notation:
[table name].[column name] -> this is how to express a COLUMN
[column name] -> this is how to express a TABLE

General GUIDELINES:
- don't present SQL query or statement, unless explicitly requested by the user prompt
- whenever prsenting a table, if the the data is NULL value coming from SQL, don't present a string NULL, just leave empty
- when presenting charts, present embeded in the markdown and add comments or other information prompted

When building SQL queries:
- use only the tables: [SpendProfile],[TimePeriod],[Ecosys_Main_Export_NonFinancial],[Ecosys_Main_Export_Financial],[SiteItem],[FundingParty],[ScenarioItem],[Escalation],[Expenditure],[Package],[Scenario],[ScenarioEstimate]
- do not consider the columns [ValidFrom] and [ValidTo] in any table
- always check the schema to verify if the exact name of the tables and columns
- month presentation format examples: Sep/2022, Aug/2015
- estimate format: 2 decimal digits with thousands separator
- do not include in any answer the values of the columns [ValidFrom] or [ValidTo]

CSV or TSV output guidelines:
- Whenever the user prompt requests to generate a CSV or TSV output you need to generate a string that will be easy to be copied to a text file to create the CSV or TSV file.
- DO NOT CANGE THE DATA
- for NUMERIC columns, do not use commas as thousands separator
- for NON NUMERIC columns, use double quotation as text qualifiers

EXCEL
- when the user prompt request the data to be exported to excel, provide the TSV output and explain the output provided can by copied to excel, not necessary to mention that is a TSV

Disambiguation:
- "expend" or "expenditure" will most likely be talking about [Ecosys_Main_Export_Financial].[value]

Steady State guidelines:
- if presenting the steady state in a chart, by default, plot a line chart with: [expenditure_year] in the X axis, [expenditure_value] as a blue line and [steady_state_curve] as a red line


Data Alternative Names:
[Ecosys_Main_Export_NonFinancial].[Program Alliance (Name)] --> "Alliance"
[Ecosys_Main_Export_NonFinancial].[Solution (Name)] --> "Solution"

Steady State 
- there is a tool to generate the Steady State data, that will probably be presented as a chart or a table
- Peak Year is the only mandatory input to generate a Steady State analysis
"""

# %%
def smt_query (q):
    msgs = { "messages":[
        SystemMessage(content= system_prompt ),
        HumanMessage(content= q)]
    }

    response = agent.invoke (msgs)

    display(Markdown( response.get("messages")[-1].content ))

    return response

# %%
def smt_query_conversation (q, conversaion_id):
    msgs = { "messages":[
        SystemMessage(content= system_prompt ),
        HumanMessage(content= q)]
    }

    reuqest_config = {"configurable": {"thread_id": conversaion_id}}

    response = agent_with_memory.invoke (msgs,reuqest_config)

    display(Markdown( response.get("messages")[-1].content ))

    return response


# %%
question = "make a bar chart of expenditure by year, provide only the chart.js JSON definition"
x = smt_query (question)
