# How to Use Guide

This repository is created to prepare datamarts in the [GCP project](https://console.cloud.google.com/bigquery?referrer=search&hl=en&project=growth-shop-prospects) for the audits of new prospecting companies.

## Prerequisites

Replace:
- `{{company_name}}` with the lowercase company name (NO abbreviations!)
- `{{customer_id}}` with the customer ID

Before running the dbt command, make sure the GCP project contains the following source dataset:
- Name: `{{company_name}}_extracts`
- Must contain the following source tables:
    - `custom_source_ga4_daily_country_marketing_channel_with_session_metrics` (_via Airbyte_)
    - `facebook_ads_insights` (_via Airbyte_)
    - `google_ads_ad_group` (_via Airbyte_)
    - `google_ads_ad_group_ad` (_via Airbyte_)
    - `google_ads_campaign` (_via Airbyte_)
    - `google_ads_keyword_view` (_via Airbyte_)
    - `p_ads_AdStats_{{customer_id}}` (_via Data transfers_)
    - `gsheet_shopify_orders_new_vs_return_2023`
    - `gsheet_shopify_orders_new_vs_return_2024` (_from UI in this [GSheet](https://docs.google.com/spreadsheets/d/1AsEXfhkNmzlCd9czmLKVZRWL4O_Q2UigNOsOahc1MpQ/edit?gid=1769170378#gid=1769170378)_)

## Build the datamart  
  
Once the source dataset with all the source tables is created, [set up dbt connected to the GCP project locally](#installation-and-cloning-the-repository) and run the following command to create the datamart:  
`./run_inside_docker.sh {{company_name}} {{customer_id}} dbt run -m +all_ads_daily_performance -t dbt_transform`

For example:
- Company name: Growth Shop  
- Customer ID: 123456789  
- `./run_inside_docker.sh growth_shop 123456789 dbt run -m +all_ads_daily_performance -t dbt_transform`

Once the `all_ads_daily_performance` datamart for the auditing company is created in the `{{company_name}}_dbt_transform` dataset, it can be connected to the GSheet report created in [this Drive folder](https://drive.google.com/drive/folders/1tcTkx-NJQGWooL7y5oRYP1lLRWemjUiA).

## Installation and Cloning the Repository

### Visual Studio Code
- Download Visual Studio Code
- To clone the repository, execute the command: `git clone git@github.com:growth-ecom-shop/growth-shop-prospects-dbt.git`
- Use `Ctrl+P` (Command+P on macOS) to quickly access files using keyword tips.

## Credentials

### Add BigQuery Access
- Get access to the corporate Google account (data@)
- Go to the service account: dbt-connection-prospects (accessible for the data@ Google account)
- Link: [Google Cloud IAM Admin](https://console.cloud.google.com/iam-admin/serviceaccounts/details/115993050948348682423/keys?hl=en&project=growth-shop-prospects)
- Generate a JSON key for the dbt service account (or use an existing one). This account already has the necessary permissions.
- Paste the JSON key into the file `gbq_creds.json.example` (located in `conf\.dbt`)
- Rename the file to `gbq_creds.json` (remove `.example`)

## Running dbt

- Install Docker (you can access the free version on the official website).
- Install dbt (you can use Git Bash or Windows PowerShell). 
    - For proper installation, use the command `pip install dbt` or, if this command doesn't work, `python3 -m pip install dbt`.
- dbt guide references:
    - [dbt Commands Documentation](https://docs.getdbt.com/reference/dbt-commands)
- Basic commands template:
    - `dbt run -m data_model_file_name -t target_database_profile_alias`
        - Explanation: This command
            1. Compiles Jinja in the file to SQL code in the target folder `data_model_file_name.sql` (you can find this compiled version in the VS Code target folder)
            2. Executes SQL code from the target folder
            3. Prints a log of the command in the console and in the log file: `dbt.log`
    - To run a model with all "parent-models" and "child-models":
        - `./run_inside_docker.sh dbt run -m +data_model_file_name+ -t target_database_profile_alias`

- If dbt commands are run without a special developer container, there is usually a prefix like:
    - `./run_inside_docker.sh`

### Command Examples

- Build model to test database schema:
    - `./run_inside_docker.sh dbt run -m 1_test_model -t test`
    - After successfully running this command, you should get the table `1_test_model` in BigQuery. You can check it by typing `1_test_model` in the BigQuery search bar.

### Workarounds

- If there is an access problem for `run_inside_docker.sh`:
    - `chmod +x run_inside_docker.sh`
    - `ls -l run_inside_docker.sh`
    - Some ideas from GPT: [GPT Link](https://chat.openai.com/share/f4f42c0c-cd4e-4da2-8d90-1dbe1bcb2d22)

# Working Folder

- The most used folder will be the `data_models` folder.

## Writing New Data Models Guideline

1. Check the data model naming convention: `data_models_naming_convention.md`
2. Add a source (if needed) in `src_airbyte.yml` (or another source `.yml` file)
3. Write SQL code
4. Run the model
5. Check in BigQuery with `SELECT * FROM target_schema.model`

# First Test Tasks

- Test model in the test schema:
    1. Create a model with the code:
        ```sql
        SELECT "Vova" AS name
        ```
    2. Run the model
    3. Check in BigQuery that the view is created
    4. Delete the relation in BigQuery

- Test model compilation:
    1. Create any model that contains `ref()` or `source()` Jinja macros
    2. Run the model
    3. Check that the compiled SQL exists in the target folder in Git
    4. Try to execute the SQL in the BigQuery console or in pushmetrics

- Add a source from Google Spreadsheet and create a BigQuery view from it

# Goal: What Business Need Does This Repo Solve?

- [Growth.Shop-VBB Data Pipeline](https://www.figma.com/board/LglAF9CjdhiS7gKqlssqr4/potential-customers-pipeline?node-id=0-1&t=dHU0f2KlRYG8DVbs-0)
