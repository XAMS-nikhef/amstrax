import setuptools

setuptools.setup(name='amstrax',
                 version='0.0.1',
                 description='XAMS on strax',
                 install_requires=['strax'],
                 python_requires=">=3.6",
                 packages=setuptools.find_packages(),
                 zip_safe = False)

