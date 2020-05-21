# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import unittest

from backports.configparser import DuplicateSectionError
from tests.compat import mock

from airflow import AirflowException
from airflow.configuration import conf, AirflowConfigException
from airflow.lineage.backend import atlas
from airflow.lineage.backend.atlas import AtlasBackend
from airflow.lineage.datasets import File
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestAtlas(unittest.TestCase):
    def setUp(self):
        try:
            conf.add_section("atlas")
            conf.add_section("atlas.extra")
        except AirflowConfigException:
            pass
        except DuplicateSectionError:
            pass
        conf.set("atlas", "username", "none")
        conf.set("atlas", "password", "none")
        conf.set("atlas", "host", "none")
        conf.set("atlas", "port", "0")
        self.atlas_backend = AtlasBackend()
        self.dag = DAG(dag_id='test_prepare_lineage', start_date=DEFAULT_DATE)

        f1 = File("/tmp/does_not_exist_1")
        f2 = File("/tmp/does_not_exist_2")
        self.inlets_d = [f1, ]
        self.outlets_d = [f2, ]

        self.op1 = DummyOperator(task_id='leave1',
                                 inlets={"datasets": self.inlets_d},
                                 outlets={"datasets": self.outlets_d},
                                 dag=self.dag)

        self.ctx = {"ti": TI(task=self.op1, execution_date=DEFAULT_DATE)}

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_lineage_send(self, atlas_mock):
        atlas._create_if_not_exists = False
        td = mock.MagicMock()
        en = mock.MagicMock()
        atlas_mock.return_value = mock.Mock(typedefs=td, entity_post=en)

        self.atlas_backend.send_lineage(operator=self.op1, inlets=self.inlets_d,
                                        outlets=self.outlets_d, context=self.ctx)

        self.assertEqual(td.create.call_count, 2)
        self.assertTrue(en.create.called)
        self.assertEqual(en.create.call_count, 1)

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_create_inlets_outlets_entities(self, atlas_mock):
        atlas._create_if_not_exists = True
        td = mock.MagicMock()
        en = mock.MagicMock()
        atlas_mock.return_value = mock.Mock(typedefs=td, entity_post=en)

        self.atlas_backend.send_lineage(operator=self.op1, inlets=self.inlets_d,
                                        outlets=self.outlets_d, context=self.ctx)
        self.assertEqual(td.create.call_count, 2)
        self.assertTrue(en.create.called)
        self.assertEqual(en.create.call_count, 3)

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_dag_fail_if_lineage_fail(self, atlas_mock):
        atlas._fail_if_lineage_error = True
        conf.set("atlas.extra", "fail_if_lineage_error", "True")
        td = mock.MagicMock()
        en = mock.Mock(side_effect=AirflowException)
        atlas_mock.return_value = mock.Mock(typedefs=td, entity_post=en)

        with self.assertRaises(AirflowException):
            self.atlas_backend.send_lineage(operator=self.op1, inlets=self.inlets_d,
                                            outlets=self.outlets_d, context=self.ctx)
