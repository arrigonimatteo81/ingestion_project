# ingestion_project


orchestrator_main.py: fa la lista dei task, in base alla tabella semaforo
|
+-> confronto tra tabella semaforo e tabella registro per capire cosa elaborare. Torna una lista di task, tutti che chiamano il processor_main.py con i vari parametri.

processor_main.py: è il main che viene chiamato da ogni task


