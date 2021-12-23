import setuptools


def open_requirements(path):
    with open(path) as f:
        requires = [
            r.split('/')[-1] if r.startswith('git+') else r
            for r in f.read().splitlines()]
    return requires


with open('README.md') as file:
    readme = file.read()


with open('HISTORY.md') as file:
    history = file.read()

requires = open_requirements('requirements.txt')
tests_requires = open_requirements('extra_requirements/requirements-tests.txt')
doc_requires = open_requirements('extra_requirements/requirements-docs.txt')

with open('README.md') as file:
    readme = file.read()

with open('HISTORY.md') as file:
    history = file.read()

setuptools.setup(name='amstrax',
                 version='1.1.1',
                 description='strax for XAMS data',
                 author='Nikhef',
                 url='https://github.com/XAMS-nikhef/amstrax',
                 long_description=readme + '\n\n' + history,
                 long_description_content_type="text/markdown",
                 setup_requires=['pytest-runner'],
                 install_requires=requires,
                 extras_require={
                     'docs': doc_requires,
                 },
                 tests_require=tests_requires,
                 python_requires=">=3.6",
                 packages=setuptools.find_packages(),
                 scripts=[],
                 classifiers=[
                     'Development Status :: 4 - Beta',
                     'License :: OSI Approved :: BSD License',
                     'Natural Language :: English',
                     'Programming Language :: Python :: 3.6',
                     'Programming Language :: Python :: 3.7',
                     'Programming Language :: Python :: 3.8',
                     'Programming Language :: Python :: 3.9',
                     'Intended Audience :: Science/Research',
                     'Programming Language :: Python :: Implementation :: CPython',
                     'Topic :: Scientific/Engineering :: Physics',
                 ],
                 zip_safe=False,
                 )
