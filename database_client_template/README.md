# How to start
1. Make sure you have all the packages listed in `Pipfile`. The script is produced using these versions so it should
work if you have exactly the same set-up. Other dependencies could work as well..
2. Complete (fill-in) the `project_config.yaml` file with the database information given by
the lecturers (Mike & Kurt). The `store` section is not necessarily needed to run the program. Of course you cannot
export data to the database without it.
3. Put your query file (`.sql` file) in the directory `queries`.
4. Write the name of the query file (without `.sql`) in the `project_config.yaml` at the `load_query` entry 
and specify how much "key"/Index columns the data has. Could be zero if there are no index-columns.
5. If you don't want to waste your time querying a big-ass dataset from the database each time you run the program
then switch on `use_cache` by passing `True` instead of `False`. It will store the queried data locally on your device
for each calendar day. So even if you have set it to `True`, it will refresh every new day for sure. WARNING:
If you change something in your `.sql`-file or other stuff that might influence your query don't forget to switch to
`False` in order to refresh your data.
6. Good to go: run `main.py` to start. Good luck, and have fun. Hopefully, the template helps you getting up to 
speed quickly using Python.


