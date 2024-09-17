# Developer Guidelines [WIP]
This pages serves as a general entry point for new and current developers. If a developer is in an onboarding process for the edw project the developers should **carefully** read the whole page.

If there is anything not clear please inform the project team to improve this page for future onboardings.

This guide contains **current best practices**, information on the CI/CD workflow, the deployment process and the security concept for the azure architecture, data factory and databricks used in this project.

We will start with general information, proceeding with more tool specific details.

## General Information

Whenever available we try to follow vendor specific guidelines. So if we have no costum rules then please look up the vendor specific guidelines.

For Azure Microsoft provides a lot of information on that where the most important ones can be found under [Azure security best practices and patterns](https://learn.microsoft.com/en-us/azure/cloud-adoption-framework/ready/landing-zone/design-areas).

For our developers I will point out the most important ones:

- Use Entra ID as centralized identity management, i.p. we want to use whenever possible **service principles/managed identities** and control their permissions using RBAC. If possbile we want to use our own service principles.
- Credentials, Passwords and in general sensitive informations should be stored in an **Azure Key Vault**.
- We want to use **naming conventions** everywere. If there is no convention we need to define one.
- We have a **DEV, UAT and PROD** environment and changes need to be done in DEV only and deployed using **Azure Pipelines**. In **DEV** all code is backed in Azure DevOps Repos to integrate changes the developer need to follow CI/CD rules.

## Azure Key Vault

As mentioned under the general information sections we want to use Azure Key Vault as Password Store. In this section we will point out more precise information on how we are using them.
We are using one Key Vault per environment and one which is environment agnostic:

-  [kv-ig-dev-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/5c771a6b-7686-4067-a58d-95551c37bf46/resourceGroups/rg-ig-common-dev-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-dev-westeurope)
-  [kv-ig-uat-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/bf75b24f-2b10-4ada-a25a-b5bec3311486/resourceGroups/rg-ig-common-uat-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-uat-westeurope)
-  [kv-ig-prod-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/5c771a6b-7686-4067-a58d-95551c37bf46/resourceGroups/rg-ig-common-dev-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-dev-westeurope/overview)
-  [kv-ig-shared-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/4b6b9329-18aa-4bf0-83f2-090c052e7fd3/resourceGroups/rg-ig-shared-westeurope/providers/Microsoft.KeyVault/vaults/kv-ig-shared-westeurope/overview)

Inside the Key Vault we are using the following naming convention:
### *type*-*subject*-*object*

For example for the on-premise navision system from infinigate we want to use the following connection string in data factory

"*Server=IGSQL03;Database=ReportsCH;Trusted_Connection=True;*".


In Azure Key Vault this can be stored as a *secret*. The subject might be *igsql03-reportsch* and the object is a *connection string.* So the secret will be named

*secret-igsql03-reportsch-connectionstring*.

Since navision has more than one database (reportsde, reportsse,...) we will have one secret per database. This can than be utilizsed inside data factory because the secrets will have the same name except the database part which serves than as a parameter so we only need one linked service.

In general if we are storing connection strings the secret should follow the same rule:
 ### secret-*servername-databasename*-connectionstring

 To give a second example:
 Suppose you want to store informations from a service principle. You need to store two things:
1. Service Principle ID
2. Service Principle Secret

If this service principle is for authentication to, say databricks, we are naming the two secrets:
1. secret-databricks-app-id
2. secret-databricks-app-value

If there is no similar example and you are not confident about a good name for your secret, ask the rest of the data team.

## Azure DevOps

All our code based development (data factory and databricks at the moment) are using one [Azure DevOps Repo ](https://dev.azure.com/InfinigateHolding/Group%20IT%20Program/_git/inf-edw). Our main branch is "main" and we are not commiting directly on the main. Insteed we are creating a new branch which should look like that "users/ksa/subject" and should contain one closed change which will be squash commited to the main branch afterwards.

### Continuous Integration
Developers practicing continuous integration merge their changes back to the main branch as often as possible. The developer's changes are validated and technical tested by the dev team. By doing so, you avoid integration challenges that can happen when waiting for release day to merge changes into the release branch.

This is done by creating a new branch from main and name it USERS/{yourname}/{subject} do a change their and create a pull request from this branch back to main.

In addition to that we are followign the following principals:

- Your Pull Requests in Dev to commit your Branch to Main can no longer be approved just by you. Any PR Must be reviewed / approved by 1 other member of the Dev Team (Kevin, Ying-Zhen, Murtaza, or Jason)
 

- We need to comment our code systematically – All of your changes to Main from your Branch Must have inline comments which as a minimum state:
/* Change Date [DD/MM/YY] , Developer Initials [JM, YZ etc.] , Branch Name */

 

- An overview email on the changes being introduced by a Branch merge must exist the PR Requestor is responsible for this. Send a quick email to KS, JM, MS, YZ with the Branch Name as the email title and a brief description covering: What the change is / Why it’s needed / What testing you’ve undertaken / Who has reviewed your PR
 

- We should be talking about PR’s and Branch Merges with the wider Team in Stand Up so that they are aware and so that wider questions can be asked / answered. We need to be clear and transparent about what we’re doing. For example, we might say in our update that;

### Continuous Delivery
Continuous delivery is an extension of continuous integration since it automatically deploys all code changes to our testing and production environment.

This is done by creating a Pull Request (PR) from main to testing and testing to release.

When we do a new PR from main to testing we add a TAG on that commit Release-vX.Y, where X indicates a new major version and y a new minor version. Typically a major version is one that is not competable with the version before in all cases and cannot be reverted easilly.
### Limitations
All changes outside of our repo, like Azure Key Vault, Metadata SQL, Databricks Workflows, Excel Sheets (ARR for example or new deltalink csvs) needs to be brought over MANUALLY.

## Azure Data Factory
In this section we will point out more precise information on how we are using data factory.
We are using one Factory per environment and one which is environment agnostic:

-  [adf-ig-dev-00-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/5c771a6b-7686-4067-a58d-95551c37bf46/resourceGroups/rg-ig-lakehouse-dev-westeurope/providers/Microsoft.DataFactory/factories/adf-ig-dev-00-westeurope)
-  [adf-ig-uat-00-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/bf75b24f-2b10-4ada-a25a-b5bec3311486/resourceGroups/rg-ig-lakehouse-uat-westeurope/providers/Microsoft.DataFactory/factories/adf-ig-uat-00-westeurope)
-  [adf-ig-prod-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/4b6b9329-18aa-4bf0-83f2-090c052e7fd3/resourceGroups/rg-ig-lakehouse-dev-westeurope/providers/Microsoft.DataFactory/factories/adf-ig-prod-westeurope)
-  [adf-ig-shared-westeurope](https://portal.azure.com/#@Infinigate.onmicrosoft.com/resource/subscriptions/4b6b9329-18aa-4bf0-83f2-090c052e7fd3/resourceGroups/rg-ig-shared-westeurope/providers/Microsoft.DataFactory/factories/adf-ig-shared-westeurope) (This is only used to register *self-hosted integration runtimes*)

Azure Data Factory is our development platform for integrating data and it has an underlying development workflow (CI/CD) in Azure DevOps Repos and an underlying deployment process in Azure Pipelines. It is important to understand, that everything the you are building needs to be "deployable".

There is a good article on how the deployment of data factory works in general: [Azure Data Factory Guidelines](https://learn.microsoft.com/en-us/azure/data-factory/continuous-integration-delivery)

To sum that up:
1. "main" the working branch and it contains all our artifacts. Inside the "data_factory" folder we are saving everything which is data factory specific.
2. "adf_publish" contains the "ARM" template which can be used for deployment to uat and prod. This ARM template will never contain sensitic information. This is one reason why *we must use Azure Key Vault*. Therefore every *linked service* needs to be setup with that in mind.
3. "global parameters" contain information that should not be deployed. For example *gpm_Environment* contains "dev" to let data factory hand over the current environment to data sets and pipelines.

The whole CI/CD workflow is described in the Azure DevOps section. In this section we proceed with more detail and best practices on artifacts inside data factory:

### Parameter
1. Parameter are named: [g]pm_"Subject", where "g" indicates if this is a global parameter.
2. Every artifact (linked service, pipeline, dataset, data flow...) which is environment specific (almost everything) should have the parameter *pm_Environment*

### Linked Services

1. Linked Services are named in all UPPERCASE: *LS_"TYPE"[_SUBJECT][_DYNAMIC]*, where _DYNAMIC indicates that the linked service itself is pointing to different sources in one environment depending on a parameter (for example "pm_DatabaseName")
2. Make use of Azure Key Vault
3. Are **not using** the default integration runtime  *AutoResolveIntegrationRuntime*

### Datasets

1. Datasets are named in all UPPERCASE: *DS_"TYPE_SOURCE_FORMAT"[_DYNAMIC]*, where _DYNAMIC indicates that the dataset is pointing to different sources in one environment depending on a parameter (for example "pm_DatabaseName")
2. We try to use *inline* datasets. This means that we dont use one dataset per data object by pointing directly to that, but we use parameters for most of the information we need to provide.

### Pipelines

1. Pipelines are named in all UPPERCASE: *PL_"XX_PHASE_ACTION"*, where "XX" indicates the order of execution, "phase" whether its Bronze, silver... and "action" what the pipelines action is.
2. Pipelines are organiszed in folders where we start by ordering after phase. In the case of bronze we are further ordering after the source we are integrating.
3. A pipeline should be build to have one specific action in mind. If you are using an iteration inside your pipeline try to not do more that that and name the pipeline XXXXX_ITERATOR. The same hold for conditions, switches and all control flow activities. In practice: a pipeline is ofter either responsable for loading one data object or manipulating the control flow.
4. Engineer from "bottom" to "top", e.g. start building the most granular pipeline (having a lot of parameters, often the copy job) and then add one inbetweet pipeline for each parameter
5. Pipelines which actually load data should be able to do a full load and a delta load to only capture new data.
6. Have a "master" pipeline per subject and a "mastermaster" pipeline orchastrating all master pipeplines on the same frequency (daily, hourly,...).