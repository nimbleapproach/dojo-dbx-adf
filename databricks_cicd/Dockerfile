FROM databricksruntime/python:14.3-LTS

COPY databricks/requirements/ /databricks/custom_requirements/

RUN /databricks/python3/bin/pip install --requirement /databricks/custom_requirements/test.txt
