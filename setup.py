from setuptools import setup

setup(name='metricq_source_dataheap',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['metricq_source_dataheap'],
      scripts=[],
      entry_points='''
      [console_scripts]
      metricq-source-dataheap=metricq_source_dataheap:run
      ''',
      install_requires=['aiohttp', 'aiohttp-cors', 'click', 'click-completion', 'click_log', 'colorama', 'dataheap', 'metricq'])
