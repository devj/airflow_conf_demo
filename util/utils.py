import string
import random


def standardize_s3_path(loc):
    if loc.startswith("s3://"):
        loc = loc[len("s3://"):]
    if loc.endswith("/"):
        loc = loc[:-1]
    return loc


def randomString(stringLength=20):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def xcom_result(task_or_id, key=None):
    if isinstance(task_or_id, str) and key == None:
        return "{{ti.xcom_pull(task_ids='" + task_or_id + "')}}"
    elif isinstance(task_or_id, BaseOperator) and key == None:
        return "{{ti.xcom_pull(task_ids='" + task_or_id.task_id + "')}}"
    elif isinstance(task_or_id, str):
        return "{{ti.xcom_pull(task_ids='" + task_or_id + "', key=\"" + key + "\")}}"
    elif isinstance(task_or_id, BaseOperator):
        return "{{ti.xcom_pull(task_ids='" + task_or_id.task_id + "', key=\"" + key + "\")}}"
    else:
        raise TypeError("Expected str or BaseOperator, but got {}".format(
            task_or_id.__class__.__name__))
