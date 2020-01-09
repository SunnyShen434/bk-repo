import os
import argparse
import time
import requests
import logging
import configparser
from config import *

parser = configparser.ConfigParser()

def parse_args():
    parser = argparse.ArgumentParser(description="Migration data from Jfrog to BKRepo.")
    parser.add_argument("-o", "--overwrite", action='store_true', dest="overwrite")
    parser.add_argument("-p", "--project", required=True, dest="project")
    parser.add_argument("-e", "--environment", default="prod", choices=["dev", "prod"], dest="env")
    parser.add_argument("-f", "--file", dest="file")
    return parser.parse_args()


def remove_prefix(text, prefix):
    return text[len(prefix):] if text.startswith(prefix) else text


def init(args):
    log_filename = "logs/{}.log".format(args.project)
    logging.basicConfig(
        level=logging.INFO,
        filename=log_filename,
        filemode="a",
        format="%(asctime)s - %(levelname)s: %(message)s"
    )
    config(args)
    if args.file:
        parser.read(args.file)

def list_jfrog_node():
    result = []
    url = jfrog_aql_url()
    auth = jfrog_auth()
    # node
    if parser.has_option("node", "list"):
        node_list = parser.get("node", "list").split()
        for node in node_list:
            try:
                data = jfrog_aql_data(node=node)
                response = requests.post(url, data=data, auth=auth)
                assert response.status_code == 200
                nodes_list = response.json()["results"]
                logging.info("Retrieve node[%s] success.", node)
                result.extend(nodes_list)
            except Exception:
                logging.exception("Retrieve node[%s] failed.", node)
    # path
    if parser.has_option("path", "list"):
        path_list = parser.get("path", "list").split()
        for path in path_list:  
            try:
                data = jfrog_aql_data(path=path)
                response = requests.post(url, data=data, auth=auth)
                assert response.status_code == 200
                nodes_list = response.json()["results"]
                logging.info("Retrieve [%s]nodes for path[%s].", total, path)
                result.extend(nodes_list)
            except Exception:
                logging.exception("Retrieve nodes for path[%s] failed.", path)
    # full
    if not parser.has_option("node", "list") and not parser.has_option("path", "list"):
        try:
            data = jfrog_aql_data()
            response = requests.post(url, data=data, auth=auth)
            assert response.status_code == 200
            nodes_list = response.json()["results"]
            result.extend(nodes_list)
        except Exception:
            logging.exception("Retrieve nodes failed.")
    return result

def check_exist(node):
    url = bkrepo_query_url(node["normalized_full_path"])
    auth = (node["created_by"], node["created_by"])
    response = requests.get(url, auth=auth)
    try:
        assert response.status_code == 200
        result = response.json()
        assert result['code'] == 0
        assert result['data']['nodeInfo']['size'] == node['size']
        return True
    except Exception:
        return False


def fetch_jfrog_file_response(node):
    url = jfrog_download_url(node["path"], node["name"])
    auth = jfrog_auth()
    response = requests.get(url, auth=auth, stream=True)
    assert response.status_code == 200
    return response

def fetch_jfrog_properties(node):
    url = jfrog_property_url(node["path"], node["name"])
    auth = jfrog_auth()
    response = requests.get(url, auth=auth)
    assert response.status_code == 200
    properties = dict()
    for key, value in response.json()['properties'].items():
        if isinstance(value, list) and len(value) > 0:
            properties[key] = value[0]
    return properties


def upload_bkrepo_file(node, jfrog_response, properties):
    url = bkrepo_upload_url(node["normalized_full_path"])
    auth = (node["created_by"], node["created_by"])
    headers = dict()
    for key, value in properties.items():
        headers["X-BKREPO-META-" + key] = value
    headers["X-BKREPO-SIZE"] = str(node["size"])
    headers["X-BKREPO-OVERWRITE"] = "true"
    response = requests.put(url, auth=auth, headers=headers, data=jfrog_response.iter_content(1024))
    assert response.status_code == 200


def migrate_node(node, current, total):
    path = remove_prefix(node["path"], "bk-custom/{}".format(args.project))
    node["normalized_full_path"] = "{}/{}".format(path, node["name"])
    jfrog_response = None
    try:
        if not args.overwrite and check_exist(node):
            logging.info("(%s/%s)[%s/%s] migration skipped: node existed.", current, total, node["path"], node["name"])
        else:
            properties = fetch_jfrog_properties(node)
            jfrog_response = fetch_jfrog_file_response(node)
            upload_bkrepo_file(node, jfrog_response, properties)
            logging.info("(%s/%s)[%s/%s] migration success.", current, total, node["path"], node["name"])
        return True
    except Exception:
        logging.exception("(%s/%s)[%s/%s] migration failed.", current, total, node["path"], node["name"])
    finally:
        if jfrog_response:
            jfrog_response.close()
    return False


def migration():
    logging.info("Start migration from jfrog [bk-custom/%s] to bkrepo [%s/custom]", args.project, args.project)
    migration_count = 0
    start_time = time.time()
    jfrog_nodes = list_jfrog_node()
    total = len(jfrog_nodes)
    logging.info("Retrieve %s nodes totally.", total)
    for index, node in enumerate(jfrog_nodes):
        if migrate_node(node, index+1, total):
            migration_count += 1
    end_time = time.time()
    elapsed_time = round(end_time - start_time, 2)
    logging.info("Migration finished, count: %s/%s, elapsed time: %ss.", migration_count, total, elapsed_time)


if __name__ == "__main__":
    args = parse_args()
    init(args)
    migration()