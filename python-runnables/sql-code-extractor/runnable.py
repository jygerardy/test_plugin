from dataiku.runnables import Runnable
import dataiku
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
import os.path as osp
import logging


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)
CODE_RECIPES = ['sql_script', 'sql_query', 'impala', 'hive', 'spark_sql']

class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()
        if self.config.get('all_projects'):
            self.projects = self.client.list_project_keys()
        else:
            self.projects = [self.project_key]

    def get_recipe_sql(self, project_handle, recipe_name, recipe_type):
        recipe_handle = project_handle.get_recipe(recipe_name)
        if recipe_type in CODE_RECIPES:
            dap = recipe_handle.get_definition_and_payload()
            return dap.get_payload()
        else:
            status = recipe_handle.get_status()
            return status.data.get('sql')

    def get_progress_target(self):
        return (len(self.projects), 'NONE')

    def run(self, progress_callback):
        output_folder_name = self.project_key + '.' + self.config.get('output_folder', '')
        folder_handle = dataiku.Folder(output_folder_name)

        bytes_io = BytesIO()
        zip_file = ZipFile(bytes_io, 'w')
        extraction_date = datetime.now().isoformat()

        recipes_per_project = {}
        project_count = 0
        for project in self.projects:
            logging.info('Checking project {}'.format(project))
            progress_callback(project_count)
            project_count += 1

            project_handle = self.client.get_project(project)
            recipes_per_project[project] = 0
            try:
                recipes = project_handle.list_recipes()
            except Exception as e:
                message = "Could not retrieve list of recipes in project {}: {}"
                recipes_per_project[project] = message.format(project,e)
                logging.warning(message.format(project, e))
                
            if len(recipes) == 0:
                message = "No recipes could be retrieved from Project: {}"
                logging.info(message.format(project))
                continue

            for recipe in recipes:
                recipe_name = recipe.get('name')
                recipe_type = recipe.get('type')
                try:
                    sql = self.get_recipe_sql(project_handle, recipe_name, recipe_type)
                except Exception as e:
                    message = "Could not retrieve sql code from recipe {} in project {}: {}"
                    logging.warning(message.format(recipe_name, project, e))
                    continue
                if sql:
                    recipes_per_project[project] += 1
                    file_name = 'sql_extract_{}/{}/{}.sql'.format(extraction_date, project, recipe_name)
                    zip_file.writestr(file_name, sql.encode('utf8'))
                else:
                    continue

        zip_file.close()
        bytes_io.seek(0)

        results = """
        <table style="width:100%">
            <tr>
                <th align="left">Project</th>
                <th align="left">Number of recipes extracted</th>
            </tr>
        """

        for k, v in recipes_per_project.iteritems():
            results += """ <tr><td>{}</td><td>{}</td></tr> """.format(k, v)
        results += '</table>'

        if max(recipes_per_project.values()):
            # at least one code could be extracted
            file_name = 'sql_extract_{}.zip'.format(extraction_date)

            with folder_handle.get_writer(file_name) as w:
                w.write(bytes_io.read())
            results += '<span>SQL code written to folder {}</span>'.format(
                output_folder_name)
        else:
            results += '<span>&#9888; No code extracted, no file written</span>'
        return results