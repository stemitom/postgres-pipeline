
<h1 align="center">Postgres Pipeline</h1>

<p align="center">
  <img alt="Github top language" src="https://img.shields.io/github/languages/top/stemitom/postgres-pipeline?color=56BEB8">

  <img alt="Github language count" src="https://img.shields.io/github/languages/count/stemitom/postgres-pipeline?color=56BEB8">

  <img alt="Repository size" src="https://img.shields.io/github/repo-size/stemitom/postgres-pipeline?color=56BEB8">

  <img alt="License" src="https://img.shields.io/github/license/stemitom/postgres-pipeline?color=56BEB8">

  <!-- <img alt="Github issues" src="https://img.shields.io/github/issues/{{YOUR_GITHUB_USERNAME}}/postgres-pipeline?color=56BEB8" /> -->

  <!-- <img alt="Github forks" src="https://img.shields.io/github/forks/{{YOUR_GITHUB_USERNAME}}/postgres-pipeline?color=56BEB8" /> -->

  <!-- <img alt="Github stars" src="https://img.shields.io/github/stars/{{YOUR_GITHUB_USERNAME}}/postgres-pipeline?color=56BEB8" /> -->
</p>

<!-- Status -->

<!-- <h4 align="center"> 
	🚧  Postgres Pipeline 🚀 Under construction...  🚧
</h4> 

<hr> -->

<p align="center">
  <a href="#dart-about">About</a> &#xa0; | &#xa0; 
  <a href="#rocket-technologies">Technologies</a> &#xa0; | &#xa0;
  <a href="#white_check_mark-requirements">Requirements</a> &#xa0; | &#xa0;
  <a href="#checkered_flag-starting">Starting</a> &#xa0; | &#xa0;
  <a href="#memo-license">License</a> &#xa0; | &#xa0;
  <a href="https://github.com/stemitom" target="_blank">Author</a>
</p>

<br>

## :dart: About ##

A simple pipeline infrastructure with ETL pipeline contained in a docker environment using apache airflow for orchestration and postgres for data warehousing

## :rocket: Technologies ##

The following tools were used in this project:

- [Apache Airflow](https://airflow.apache.org/)
- [Postgres](https://www.postgresql.org/)
- [Docker](https://docs.docker.com/compose/)

## :white_check_mark: Requirements ##

Before starting :checkered_flag:, you need to have [Git](https://git-scm.com) and [Node](https://nodejs.org/en/) installed.

## :checkered_flag: Starting ##

```bash
# Clone this project
$ git clone https://github.com/stemitom/postgres-pipeline

# Access
$ cd postgres-pipeline

# Install dependencies in your environment
$ pip install -r requirements.txt

# Run the project
$ docker-compose up -d

# The airflow server will initialize at <http://localhost:8080>
```

## :memo: License ##

This project is under license from MIT. For more details, see the [LICENSE](LICENSE.md) file.


Made with :heart: by <a href="https://github.com/stemitom" target="_blank">Temiloluwa Samuel</a>

&#xa0;

<a href="#top">Back to top</a>
