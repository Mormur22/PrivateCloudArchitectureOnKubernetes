

estudiante@master:~/monitoring$ kubectl get svc -n monitoring
NAME                 TYPE       CLUSTER-IP       EXTERNAL-IP   PORT(S)          AGE
prometheus-service   NodePort   10.107.202.121   <none>        8080:30000/TCP   25h
estudiante@master:~/monitoring$ kubectl get svc -n sharded-cluster
No resources found in sharded-cluster namespace.
estudiante@master:~/monitoring$ kubectl get svc -n mongo-sharded
NAME                       TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)           AGE
grafana                    NodePort    10.107.38.89     <none>        80:31363/TCP      6d20h
mongodb-configserver       ClusterIP   None             <none>        27019/TCP         9d
mongodb-exporter-service   ClusterIP   10.102.146.179   <none>        9216/TCP          30h
mongodb-mongos             NodePort    10.108.154.169   <none>        27017:30159/TCP   9d



ssh -L 30000:localhost:30000 -L 31363:localhost:31363 -L 30159:localhost:30159 estudiante@147.96.81.119
