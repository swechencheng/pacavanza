import setuptools

setuptools.setup(
    name="pacavanza",
    version="0.1.0",
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "avanza-api",
        "curl_cffi",
        "TA-Lib",
        "redis",
        "fastapi",
        "uvicorn",
        "aiohttp",
    ],
    entry_points={"console_scripts": ["run-pacavanza = pacavanza.backend:main"]},
)
