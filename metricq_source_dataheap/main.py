#!/usr/bin/env python3
# Copyright (c) 2018, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#     * Neither the name of metricq nor the names of its contributors
#       may be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import asyncio
import logging

import click
import click_log
import click_completion

from dataheap.dhlib import dhClient
import metricq

logger = metricq.get_logger()

click_log.basic_config(logger)
logger.setLevel('INFO')
logger.handlers[0].formatter = logging.Formatter(
    fmt='%(asctime)s %(threadName)-16s %(levelname)-8s [%(name)-20s] %(message)s')

click_completion.init()


class Bridge(metricq.Source):
    def __init__(self, *args, **kwargs):
        logger.info('initializing Dataheap-MetricQ-Bridgeß')
        super().__init__(*args, **kwargs)
        self.dataheap_client = None
        # key: Dataheap id, value: MetricQ metric name
        self.dataheap_metrics = dict()
        self.dataheap_host = None
        self.dataheap_port = None
        # key: Dataheap counter name, value: MetricQ metric name
        self.metric_mapping = None

    def dataheap_data(self, dataheap_id, dataheap_timestamp, value):
        # This is called by the dataheap thread
        metricq_name = self.dataheap_metrics[dataheap_id]
        logger.debug('received Dataheap data {} ({}) {} {}', dataheap_id, metricq_name, dataheap_timestamp, value)
        f = asyncio.run_coroutine_threadsafe(
            self.send(metricq_name, dataheap_timestamp / 1000.0, value),
            self.event_loop
        )
        exception = f.exception(10)
        if exception:
            logger.error('Failed to send data {}', exception)
        logger.debug('sent data to MetricQ complete')

    def dataheap_disconnected(self):
        logger.error('disconnected from dataheap server, reconnecting')
        self.dataheap_connect()

    def dataheap_connect(self):
        if self.dataheap_client:
            self.dataheap_client.close()
            self.dataheap_client = None
            self.dataheap_metrics = dict()

        self.dataheap_client = dhClient()
        self.dataheap_client.init_monitor(host=self.dataheap_host, port=self.dataheap_port,
                                          dataCb=self.dataheap_data, discCb=self.dataheap_disconnected)
        logger.info('connected to Dataheap server {}:{}', self.dataheap_host, self.dataheap_port)

        for dataheap_name, metricq_name in self.metric_mapping.items():
            dataheap_id = self.dataheap_client.subscribe(dataheap_name)
            if not dataheap_id:
                logger.error('invalid Dataheap counter: {}', dataheap_name)

            self.dataheap_metrics[dataheap_id] = metricq_name
            logger.info('subscribed to Dataheap counter {} as {} -> {}', dataheap_name, dataheap_id, metricq_name)

    @metricq.rpc_handler('config')
    async def config(self, dataheapHost, dataheapPort, metrics, **kwargs):
        self.dataheap_host = dataheapHost
        self.dataheap_port = dataheapPort
        self.metric_mapping = metrics
        self.declare_metrics(list(metrics.values()))

    async def task(self):
        self.dataheap_connect()


@click.command()
@click.option('--server', default='amqp://localhost/')
@click.option('--token', default='source-py-dummy')
@click_log.simple_verbosity_option(logger)
def run(server, token):
    bridge = Bridge(token=token, management_url=server)
    bridge.run()
