import setuptools

setuptools.setup(
    name="pacavanza",
    version="0.1.0",
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "avanza-api",
        "flask",
        "plotly",
        "dash",
        "curl_cffi",
        "TA-Lib",
        "redis[async]",
    ],
    entry_points={
        "console_scripts": ["run-pacavanza = pacavanza.run_dashboard:main"]
    },
)
