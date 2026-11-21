import unittest

from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import test_cycle


class DagValidation(unittest.TestCase):
    def test_all_dags_parse(self):
        bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
        self.assertFalse(bag.import_errors, bag.import_errors)
        self.assertEqual(len(bag.dags), 6)
        for dag in bag.dags.values():
            test_cycle(dag)
            self.assertTrue(dag.tasks)
            self.assertTrue(all(task.retries == 3 for task in dag.tasks))
