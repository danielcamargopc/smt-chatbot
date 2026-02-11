

from sqlalchemy.engine import URL

from langchain.agents import create_agent
from langchain.tools import tool

from langchain_openai import AzureChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage


# - - - - - - - - - - - - - - fixerror
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.types import Unicode
MSDialect.ischema_names['sysname'] = Unicode


from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit

# from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.checkpoint.memory import InMemorySaver

import json
import pyodbc

import ast


connection_url = URL.create(
    "mssql+pyodbc",
    username = "biireadonly",
    password = "smtP@ssword",
    host     = "sql-d-lxr-aue-crs01.database.windows.net",
    database = "db-d-lxr-aue-smt",
    query={"driver": "ODBC Driver 18 for SQL Server","TrustServerCertificate": "yes"}
)

connection_url_w = URL.create(
    "mssql+pyodbc",
    username = "lxrpadmin",
    password = "Localadmin421!@",
    host     = "sql-d-lxr-aue-crs01.database.windows.net",
    database = "db-d-lxr-aue-smt",
    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}
)

# - - - - - - - - - - - - - -



llm = AzureChatOpenAI(
    openai_api_version = "2025-01-01-preview",
    ##&&
    azure_endpoint     = "https://zafar-m6yaen7x-eastus2.cognitiveservices.azure.com/",
    api_key            = "4re6rNoMruOiFyzsIGc3blvkHk6wNFlruRvC3j24Qk8I0ORJquRiJQQJ99BBACHYHv6XJ3w3AAAAACOGmtoa",
    azure_deployment = "gpt-5.1-chat",
)

db = SQLDatabase.from_uri( connection_url )
dbw = SQLDatabase.from_uri( connection_url_w )

# - - - - - - - - - - - - - -

# print ("\nsmt ai core functions\n","# "*12, sep = "")

# r = llm.invoke ( "what is capital of china?")
# # print (r.get("messages")[-1])
# # print (r.content)

# query = """SELECT TOP (1000) [PackageId]
#       ,[EcoSysPackageId]
#       ,[PackageName]
#       ,[PackageStatus]
#       ,[CreatedDate]
#       ,[CreatedUser]
#       ,[ModifiedDate]
#   FROM [dbo].[Package]"""

# # result = db.run(query)        # option 1
# result = db.run_no_throw(query)  # option 2

# print(result)


# - - - - - - - - - - - - - - AGENTS DEFINITION

toolkit = SQLDatabaseToolkit(db=dbw, llm=llm)
sql_tools = toolkit.get_tools()

# all_tools = sql_tools + [provide_expenditure_query, provide_steady_state_query,chart9]
all_tools = sql_tools 

agent = create_agent (
    model = llm,
    tools = all_tools
)

agent_with_memory = create_agent (
    model = llm,
    tools = all_tools,
    checkpointer=InMemorySaver()
)


# - - - - - - - - - - - - - - PROMPTS FUNCTION

system_prompt = "You are a helpful assistant."

# [Ecosys_Main_Export_NonFinancial],[Ecosys_Main_Export_Financial],

system_prompt = """You are a helpful assistant.

Provide answers in markdown

Tables and Columns notation:
[table name].[column name] -> this is how to express a COLUMN
[table name] -> this is how to express a TABLE
[].[column name] -> this is how to express to a COLUMN, regardless the table where it is

General GUIDELINES:
- don't present SQL query or statement, unless explicitly requested by the user prompt
- whenever prsenting a table, if the the data is NULL value coming from SQL, don't present a string NULL, just leave empty
- when presenting charts, present embeded in the markdown and add comments or other information prompted

When building SQL queries:
- use only the tables: [SpendProfile],[TimePeriod],[SiteItem],[FundingParty],[ScenarioItem],[Escalation],[Expenditure],[Package],[Scenario],[ScenarioEstimate]
- the columns [].[ValidFrom] and [].[ValidTo] must not be used
- always check the schema to verify if the exact name of the tables and columns"""


system_prompt = """You are a helpful assistant.

Provide answers in MARKDOWN

# Tables and Columns notation:
* [table name].[column name] -> this is how to mention a COLUMN
* [column name] -> this is how to mention a TABLE
* [].[column name] -> this is how to mention a genericaly a column name, regardless the table, it may be refering to a column name that may appears in several table

# General Guidelines:
* do not present the SQL statement in the response, __unless explitly requested by the user prompt__
* do not ask SQL tecnchal apects to the user, like, what is the best option to do a insert or a update. You decide what is better
* if it is needed to run a SQL STATEMENT, run it, not necessary confirmation
* __unless explitly requested by the user prompt__, do not include in the response any values from the columns below, **EVEN IF MENTIONED BY THE USER PROMPT**:
[].[ValidFrom], [].[ValidTo],
[Escalation].[EscalationSerieId], [Escalation].[EscalationVersionId], [Expenditure].[ExpenditureId], [Expenditure].[SiteItemId],
[Expenditure].[TimePeriodId], [Package].[PackageId], [Scenario].[ScenarioId], [Scenario].[ScenarioCode], [Scenario].[SnapShotCode], [ScenarioEstimate].[ScenarioEstimateId],
[ScenarioEstimate].[ScenarioItemId], [ScenarioEstimate].[TimePeriodId], [ScenarioItem].[ScenarioId], [ScenarioItem].[ScenarioItemId],
[ScenarioItem].[PackageId], [ScenarioItem].[ParentScenarioItemId], [SiteItem].[ParentSiteItemId], [Site].[PackageId], [Site].[SiteId],
[SiteItem].[SiteId], [SiteItem].[SiteItemId], [SpendProfile].[Id], [TimePeriod].[TimePeriodId]
* __unless explitly requested by the user prompt__, do not mention SQL tecnichal elements, like tables or column names

# data dictionary and expressions
* if user prompt says: "scenario XYZ" it means "scenario which **scenario name** is XYZ" 
* if user prompt says: "scenario value", "scenario estimate" or "scenario estimation" it is talking about the SUM of [ScenarioEstimate].[ScenarioEstimateValue]
* if user prompt says: "XYZ in a scenario" it is talking about [ScenarioItem].[ScenarioItemName] = XYH
* "work" may be regard a scenario item
* "delay or advance the start of a work" means "shift all the estimates of a scenario item"
* "Unawarded packages" means Packages where Package Status is not "Award"
* "Unawarded works" means Packages where Package Status is not "Award" plus Scenario Items where Item Type is "plug"

# SQL STATEMENTS GUIDELINES
* use only the tables: [SpendProfile],[TimePeriod],[SiteItem],[FundingParty],[ScenarioItem],[Escalation],[Expenditure],[Package],[Scenario],[ScenarioEstimate]
* do not use columns [].[ValidFrom] and [].[ValidTo] in any table

## SQL statement values verification
- for EVERY text in the user prompt needed to be used in a query you should do EXACT VERIFICATION, which is: Search by SIMILARITY in the existing values in the appropriated column. Create a final query using equalities, as the example below:
    [Table Name].[Column Name] = 'value find by SIMILARITY Search'

## Guidelines for INSERT statements creation:
* if the target table has the column [].[CreatedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[ModifiedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[CreatedUser], use the value 'CHATBOT'
* if the target table has the column [].[ModifiedUser], use the value 'CHATBOT'
* if necessary to create a GUID, use the NEWID() function from SQL SERVER. NEWID() should be used inline with the statement, otherwise, if stored in a variable, it provides repited values and it doesn't work as GUID.

## Guidelines for UPDATE statements creation:
* if the target table has the column [].[ModifiedDate], use the current datetime, using the GETDATE() function from SQL SERVER
* if the target table has the column [].[ModifiedUser], use the value 'CHATBOT'

## manipulation of column [].[TimePeriodId] and MONTH representation:
* column [ScenarioEstimate].[TimePeriodId] and column [TimePeriod].[TimePeriodId] are integer that represents the first day of a reference month. The SQL SERVER snipped below calculates this date:
    DATEADD(DAY, [TimePeriodId] - 1e6 -2 ,0)
* the SQL SERVER expression below converts the **TimePeriodId** to the first day of Reference Month
    DATEADD(DAY, TimePeriodId - 1000002, '1900-01-01')

## Scenario Item hierarchical structure:
* when quering values regarding each ScenarioItem, always also consider the whole hierarchy below the refered ScenarioItem. The hierarchy is defined by the columns [ParentScenarioItemId] and [ScenarioItemId]

## handling records in table [ScenarioEstimate]:
* there should be at most 1 row for the same combination of [ScenarioEstimate].[TimePeriodId] and [ScenarioEstimate].[ScenarioItemId]
* if necessary to do any ADDITION or SUBTRACTION of [ScenarioEstimate].[ScenarioEstimateValue] do by update the existing column if it already exists
* if you generate any row (because a INSERT or a UPDATE operation) with [ScenarioEstimate].[ScenarioEstimateValue] = 0.00, delete this row

## handling records in table [ScenarioEstimate] - **moving** or **shifting** estimates a number of months:
* the SQL SERVER T-SQL statement shifts a selection of Scenario Estimates 17 months later (forward), but do not use 17 as default, necessarily extract this information from user prompt
* adapt this statement as per user prompt needs
* variable @shit_months is the number of months to be shifted
* use @shit_months NEGATIVE to shift months bacward
* temp table #source_scenario_estimate stores the selection of Scenario Estimates to be shifted, adapt as per user promp needs

```t-sql
DECLARE 
@mx_i INT,
@i INT=1,
@shit_months INT = 17,
@src_date_serial INT,
@tgt_date_serial INT,
@execution_dt DATETIME=GETDATE(),
@chatbot_user VARCHAR(10)='CHATBOT';
DROP TABLE IF EXISTS #source_scenario_estimate;SELECT IIF(@shit_months>0,DENSE_RANK()OVER(ORDER BY TimePeriodId DESC),DENSE_RANK()OVER(ORDER BY TimePeriodId ASC))Excution_Order,se.* INTO #source_scenario_estimate FROM Scenario s JOIN ScenarioItem si ON si.ScenarioId=s.ScenarioId JOIN ScenarioEstimate se ON se.ScenarioItemId=si.ScenarioItemId WHERE s.ScenarioId='ABCD-EFGH' AND se.TimePeriodId IN(1047939,1047908,1047880) ORDER BY TimePeriodId DESC;SELECT @mx_i=MAX(Excution_Order)FROM #source_scenario_estimate;WHILE @i<=@mx_i BEGIN SELECT @src_date_serial=MAX(TimePeriodId)FROM #source_scenario_estimate WHERE Excution_Order=@i;SET @tgt_date_serial=DATEDIFF(DAY,'1900-01-01',DATEADD(MONTH,@shit_months,DATEADD(DAY,@src_date_serial-1000000-2,'1900-01-01')))+1000000+2;MERGE INTO ScenarioEstimate tgt USING(SELECT*FROM #source_scenario_estimate WHERE Excution_Order=@i)src ON tgt.ScenarioItemId=src.ScenarioItemId AND tgt.TimePeriodId=@tgt_date_serial AND src.TimePeriodId=@src_date_serial WHEN MATCHED THEN UPDATE SET tgt.ScenarioEstimateValue=src.ScenarioEstimateValue+tgt.ScenarioEstimateValue,tgt.ModifiedUser=@chatbot_user,tgt.ModifiedDate=@execution_dt WHEN NOT MATCHED BY TARGET THEN INSERT(ScenarioEstimateId,ScenarioItemId,TimePeriodId,ScenarioEstimateValue,CreatedDate,CreatedUser,ModifiedDate,ModifiedUser)VALUES(NEWID(),src.ScenarioItemId,@tgt_date_serial,src.ScenarioEstimateValue,@execution_dt,@chatbot_user,@execution_dt,@chatbot_user);DELETE FROM ScenarioEstimate WHERE ScenarioEstimateId IN(SELECT ScenarioEstimateId FROM #source_scenario_estimate WHERE Excution_Order=@i);SET @i+=1;END;
```

# PRESENTATION guidelines
* if presenting a MONTH , present it sorted by month
* months should be in the format MMM/YYYY
* if presenting the value [ScenarioEstimate].[ScenarioEstimateValue] aggregated or not, show with 2 decimal digits, with thousands separator

# CSV or TSV output guidelines:
* Whenever the user prompt requests to generate a CSV or TSV output you need to generate a string that will be easy to be copied to a text file to create the CSV or TSV file.
* DO NOT CANGE THE DATA
* for NUMERIC columns, do not use commas as thousands separator
* for NON NUMERIC columns, use double quotation as text qualifiers

# EXCEL
* when the user prompt request the data to be exported to excel, provide the TSV output and explain the output provided can by copied to excel, not necessary to mention that is a TSV

# Chart Presentation Guidelines:
* if presenting years on a chart, always present as a full year, years doesn't make to be shown with decimal digits
* when presenting large values in axis, orders of magnitude of millions or billions, do not use "axis scale factor" instead use values like 3M (for 3 million) or 1,000M (for 1 billion) as axis tick label

# Scenario Hierarchical Structure
* when asking about a Scenario Item, the user prompt may be about only the Scenario Item mentioned OR about the whole hierarchy below it, 
it is necessary to understand what the user needs. If nothing is said, assume the user wants the whole hierarchy

# CUSTOM OPERATIONS

## Scenario COPY or DUPLICATION
* it is mandatory to provide a **new Scenario Name** and the source **source scenario name**, but the **new Scenario Name** must be unique when created
* If **Scenario Code** or **Scenario Snapshot Code** are not provided by user prompt, create one that follow the same pattern of the values already existing in the database table for the same column but keep the column unique. Do not need to ask confirmation about your selection of these values
* the SQL SERVER statement below is an example that:
copies the existing scenario "source scenario name ABC" and creates the scenario "new scenario name XYZ" with scnenario code "new_scenario_code_TUV" snapshot code "new_scenario_ss_code_DEF" 

```t-sql
DECLARE
@NEW_ScenarioName VARCHAR(100) = 'new scenario name XYZ',
@ScenarioName VARCHAR(100) = 'source scenario name ABC',
@NEW_ScenarioId UNIQUEIDENTIFIER = NEWID(),
@NEW_ScenarioCode VARCHAR(100) = 'new_scenario_code_TUV',
@NEW_SnapShotCode VARCHAR(100) = 'new_scenario_ss_code_DEF',
@creation_dt DATETIME = GETDATE()

INSERT INTO Scenario (ScenarioId,ScenarioCode,SnapShotCode,ScenarioName,Description,LockedBy,LockedAt,Attributes,Notes, CreatedDate,CreatedUser,ModifiedDate,ModifiedUser) select @NEW_ScenarioId, @NEW_ScenarioCode, @NEW_SnapShotCode, @NEW_ScenarioName, Description, LockedBy, LockedAt, Attributes, Notes, @creation_dt, 'CHATBOT', @creation_dt, 'CHATBOT' FROM Scenario WHERE ScenarioName = @ScenarioName select si.ScenarioItemId ,NEWID() as NEW_ScenarioItemId ,si.ParentScenarioItemId ,CAST( NULL AS uniqueidentifier) as NEW_ParentScenarioItemId ,si.PackageId ,si.ScenarioItemName ,si.ExpenditureType ,si.FundingSplit ,si.EscalationSerieId ,si.SpendProfile ,si.StartDate ,si.ItemType ,si.SpendProfileNoOfMonths ,si.Months ,si.Amount INTO #si_tmp from Scenario s inner join ScenarioItem si on si.ScenarioId = s.ScenarioId where s.ScenarioName = @ScenarioName UPDATE s SET NEW_ParentScenarioItemId = si_lookup.NEW_ScenarioItemId FROM #si_tmp s INNER JOIN #si_tmp si_lookup on si_lookup.ScenarioItemId = s.ParentScenarioItemId WHILE EXISTS (SELECT * FROM #si_tmp WHERE NEW_ScenarioItemId NOT IN (select ScenarioItemId FROM ScenarioItem) ) BEGIN INSERT INTO ScenarioItem (ScenarioId, ScenarioItemId, ParentScenarioItemId, PackageId, ScenarioItemName, ExpenditureType, FundingSplit, EscalationSerieId, SpendProfile, StartDate, ItemType, SpendProfileNoOfMonths, Months, Amount, CreatedDate, CreatedUser, ModifiedDate, ModifiedUser ) select @NEW_ScenarioId, NEW_ScenarioItemId, NEW_ParentScenarioItemId, PackageId, ScenarioItemName, ExpenditureType, FundingSplit, EscalationSerieId, SpendProfile, StartDate, ItemType, SpendProfileNoOfMonths, Months, Amount, @creation_dt, 'CHATBOT', @creation_dt, 'CHATBOT' FROM #si_tmp sit WHERE sit.NEW_ScenarioItemId NOT IN (select ScenarioItemId FROM ScenarioItem) AND (sit.NEW_ParentScenarioItemId IN (select ScenarioItemId FROM ScenarioItem) OR sit.NEW_ParentScenarioItemId IS NULL ) END INSERT INTO ScenarioEstimate (ScenarioEstimateId,ScenarioItemId,TimePeriodId,ScenarioEstimateValue, CreatedDate, CreatedUser, ModifiedDate, ModifiedUser) SELECT NEWID(), sit.NEW_ScenarioItemId, se.TimePeriodId, se.ScenarioEstimateValue, @creation_dt, 'CHATBOT', @creation_dt, 'CHATBOT' FROM Scenario s INNER JOIN ScenarioItem si on si.ScenarioId = s.ScenarioId INNER JOIN ScenarioEstimate se on se.ScenarioItemId = si.ScenarioItemId INNER JOIN #si_tmp sit on sit.ScenarioItemId = se.ScenarioItemId WHERE s.ScenarioName = @ScenarioName
```

## Steady State Analisys
* Steady State Analysis will probably be requested as a table or as a chart most likely as line chart
* the mandatory inputs are at least:
    * Scenario Name - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
    * Peak Year - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
    * Escalion Name - do not make assumptions, the user prompt must explicitly inform this information for the Steady State analysis
    * Escalation Reference Date, if nothing informed, use by default the latest available for the provided Escalation
    * the T-SQL script below, provides the Steady State Curve for **scenario**: ABCD_ABCD, **peak year**: 2022, **escalation**: XXYYZZ, **escalation reference date**: 2025-08-13

```t-sql
DECLARE @PeakYear INT = 2022

;with escalation_tb as 
(select YEAR(EscalationTargetDate) as Year, AVG(EscalationMultiplier) as Multiplier from Escalation where EscalationName = 'XXYYZZ' and EscalationRefDate = '2025-08-13' group by YEAR(EscalationTargetDate) )

,scenario_value_tb as
(
select 
YEAR(DATEADD(DAY, se.TimePeriodId - 1e6 -2 ,0)) as Year,
SUM (se.ScenarioEstimateValue) Estimate_Value
from Scenario s
inner join ScenarioItem si on si.ScenarioId = s.ScenarioId
inner join ScenarioEstimate se on se.ScenarioItemId = si.ScenarioItemId
where s.ScenarioName = 'ABCD_ABCD'

GROUP BY YEAR(DATEADD(DAY, se.TimePeriodId - 1e6 -2 ,0))
)
select 
scenario_value_tb.*,
escalation_tb.Multiplier as Escalation_Multiplier,
CASE 
WHEN scenario_value_tb.Year < @PeakYear THEN NULL
WHEN scenario_value_tb.Year = @PeakYear THEN scenario_value_tb.Estimate_Value
WHEN escalation_tb.Multiplier IS NULL then scenario_value_tb.Estimate_Value
ELSE escalation_tb.Multiplier * scenario_value_tb.Estimate_Value
END Steady_State_Curve

from scenario_value_tb left join escalation_tb on scenario_value_tb.Year = escalation_tb.Year
order by Year
```
"""

def smt_chatbot_request ( r_json ):

    # r_json = eval(r_json)

    user_prompt     = r_json.get("user_prompt")
    conversation_id = r_json.get("conversation_id")
    scenario_id     = r_json.get("scenario_id")
    user_prompt_id  = r_json.get("user_prompt_id")
    conversation_id = r_json.get("conversation_id")
    user_id         = r_json.get("user_id")

    if scenario_id:
        result = db.run(f"select top 1 ScenarioName from Scenario where ScenarioId = '{scenario_id}'")
        result_dataset = ast.literal_eval(result)[0][0]  
        scenario_id_prompt = f"This question is mainly about scenario \"{result_dataset}\" \n\n"
    else:
        scenario_id_prompt = ""
    
    user_prompt = scenario_id_prompt + user_prompt

    msgs = { "messages":[
        SystemMessage(content = system_prompt ),
         HumanMessage(content = user_prompt)]
    }

    reuqest_config = {"configurable": {"thread_id": conversation_id}}

    response = agent_with_memory.invoke (msgs, reuqest_config)

    ai_internal_messge = None

    ai_response = response.get("messages")[-1].content

    ai_response = ai_response.replace("'","''")

    if user_prompt:
        user_prompt = user_prompt.replace("'","''")

    columns_to_insert = [
        ("AiResponse"       , ai_response)
        ,("AiInternalMessage", ai_internal_messge)
        ,("ConversationId"   , conversation_id)
        ,("UserPromptId"     , user_prompt_id)
        ,("UserId"           , user_id)
        ,("UserPrompt"       , user_prompt)
        ,("ScenarioId"       , scenario_id)
    ]

    columns_headers = [x[0] for x in columns_to_insert if x[1] != None]
    columns_values  = ["'" + x[1] + "'" for x in columns_to_insert if x[1] != None]

    columns_headers = ["CreatedDate"] + columns_headers
    columns_values  = ["GETDATE()"]   + columns_values

    columns_headers_sql = ",".join(columns_headers)
    columns_values_sql  = ",".join( columns_values)

    # print (columns_headers , "\n@ @ @ @ @ @ \n" , columns_values)


    sql_statement = f"""
    SET NOCOUNT ON;

    INSERT INTO dbo.ChatbotAiMessage({columns_headers_sql})
    VALUES
    ({columns_values_sql})
    
    SELECT cast(SCOPE_IDENTITY() as int) as ChatbotAiMessageId
    """

    # print (sql_statement)
    # ChatbotAiMessageId	CreatedDate	AiResponse	AiInternalMessage	ConversationId	UserPromptId	UserId	UserPrompt	ScenarioId

    # sql_insert_statement = f"""
    # SET NOCOUNT ON;

    # INSERT INTO dbo.ChatbotAiMessage(CreatedDate,AiResponse)
    # VALUES
    # (GETDATE(),'{ai_response}')
    
    # SELECT cast(SCOPE_IDENTITY() as int) as ChatbotAiMessageId
    # """

    db_resultset = dbw.run_no_throw(sql_statement)

    db_resultset = ast.literal_eval(db_resultset)

    db_resultset = db_resultset[0][0]

    return db_resultset





# print (20 * " -> ")
# iii = {"user_prompt":"how many scenarios are there?","conversation_id":"1234"}
# # iii = json.dumps (iii)
# zzz =  smt_chatbot_request ( iii )
# print (zzz)