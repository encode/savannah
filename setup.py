from setuptools import setup

setup(
    name='savannah',
    version='0.1',
    py_modules=['savannah'],
    install_requires=[
        'click',
    ],
    entry_points='''
        [console_scripts]
        savannah=savannah:cli
    ''',
)
