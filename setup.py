from setuptools import setup, find_packages

setup(
    name="wildfires",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=1.5.0',
        'geopandas>=0.12.0',
        'numpy>=1.23.0',
        'folium>=0.14.0',
        'branca>=0.6.0',
        'shapely>=2.0.0',
        'pyproj>=3.4.0',
        'fiona>=1.9.0',
        'tqdm>=4.65.0',
        'matplotlib>=3.6.0',
        'seaborn>=0.12.0',
        'jupyter>=1.0.0',
        'pytest>=7.3.0'
    ],
    python_requires='>=3.8',
) 