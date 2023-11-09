nel DriverBigData setto i job1 e job2.

1) job1 viene usato per contare quante occorrenze ci sono delle coppie di prodotti, l'output si trova in /OutFiles/f1
2) job2 è utile per fare la topK di ciò che è stato generato dal job1 (l'input sarà l'output del precedente job)