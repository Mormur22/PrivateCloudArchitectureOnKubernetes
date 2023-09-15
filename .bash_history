sudo openvpn --config /etc/openvpn/server.conf
sudo chown root:root /etc/openvpn/ta.key
sudo openvpn --config /etc/openvpn/server.conf
sudo nano /etc/openvpn/server.conf
sudo openvpn --config /etc/openvpn/server.conf
sudo nano /etc/openvpn/server.conf
sudo openvpn --config /etc/openvpn/server.conf
sudo nano /etc/openvpn/server.conf
sudo openvpn --config /etc/openvpn/server.conf
ls
cd easyrsa
cd ..
ls
ls -l /etc/openvpn/ca.crt
ls -l /etc/openvpn/server.crt
ls -l /etc/openvpn/server.key
cd easy-rsa/
ls
cd pki/
ls
cd ..
cd pki
cd certs_by_serial/
ls
cd ..
cd private
ls
mkdir /etc/openvpn/keys
sudo mkdir /etc/openvpn/keys
mv ./ca.key /etc/openvpn/keys
sudo mv ./ca.key /etc/openvpn/keys
sudo mv ./server.key /etc/openvpn/keys
cd ..
ls
mv ./ca.crt /etc/openvpn/keys
sudo mv ./ca.crt /etc/openvpn/keys
sudo nano /etc/openvpn/server.conf
sudo chmod 600 /etc/openvpn/server.key
sudo chmod 600 /etc/openvpn/keys/server.key
sudo chmod 600 /etc/openvpn/keys/ca.key
sudo chmod 600 /etc/openvpn/keys/ca.crt
sudo openvpn --config /etc/openvpn/server.conf
sudo nano /etc/openvpn/server.conf
cd /etc/openvpn/
ls
cd keys
ls
ls -la
cd ~/easy-rsa/pki
ls
cd private
ls
cd ..
source vars
./build-server-full server nopass
./easyrsa --version
cat vars
./easyrsa build-server-full server nopass
cp /etc/openvpn/keys/ca.crt 
cp /etc/openvpn/keys/ca.crt ./
ls
./easyrsa build-server-full server nopass
ls
./easyrsa build-server-full server nopass
./easyrsa init-pki
cd pki
ls
./easyrsa build-server-full server nopass
cd ..
ls
./easyrsa build-server-full server nopass
./easyrsa build-server-full 
./easyrsa build-server-full --help
sudo systemctl stop  openvpn@server
cd ..
systemctl status mongo_exporter
systemctl status mongo-exporter
kubectl get namespaces
kubectl get pods -n prometheus
kubectl get pods -n mongodb
kubectl get pods -n mongo-sharded
kubectl get pods -n test-ns
sudo systemctl status mongodb_exporter.service
cd /usr/local/bin/mongodb_exporter
cd /usr/local/bin/
ls
nano mongodb_exporter
sudo nano /lib/systemd/system/mongodb_exporter.service
sudo systemctl daemon-reload
sudo systemctl restart mongodb_exporter.service
sudo systemctl status mongodb_exporter.service
sudo journalctl -u mongodb_exporter.service
sudo nano /lib/systemd/system/mongodb_exporter.service
/usr/local/bin/mongodb_exporter --mongodb.uri=mongodb://test:testing@147.96.81.119:30159 --mongodb.collstats-colls=Test.Listings --discovering-mode --mongodb.direct-connect --collect-all
/usr/local/bin/mongodb_exporter --mongodb.uri=mongodb://test:testing@147.96.81.119:30159  --discovering-mode --mongodb.direct-connect --collect-all
/usr/local/bin/mongodb_exporter --help
sudo nano /lib/systemd/system/mongodb_exporter.service
sudo systemctl restart mongodb_exporter.service
sudo systemctl daemon-reload
sudo systemctl restart mongodb_exporter.service
sudo systemctl status mongodb_exporter.service
kill -HUP $(pidof prometheus)
curl -X POST http://localhost:9090/-/reload
/usr/local/bin/mongodb_exporter --help
sudo systemctl stop mongodb_exporter.service
cd ~
ls
cd mongodb_exporter-0.37.0.linux-amd64
ls
podman run -d -p 9216:9216 -p 30159:30159 percona/mongodb_exporter:0.20 --mongodb.uri=mongodb://147.96.81.119:30159 --mongodb.collstats-colls=Test.Listings --discovering-mode --mongodb.direct-connect --collect-all  
kubectl get pods
ls
nano mongodb_exporter.yaml
mongodb_exporter_linux_amd64/mongodb_exporter --mongodb.uri=mongodb://147.96.81.119:30159 --mongodb.collstats-colls=Test.Listings --discovering-mode --mongodb.direct-connect --collect-all
cd ..
ls
cd mongodb-exporter
ls
podman ps
podman ps -a
podman logs 06b56d0d7dd5
--mongodb.uri=mongodb://147.96.81.119:30159 --discovering-mode --mongodb.direct-connect --compatible-mode --enable.dbstats --mongodb.collstats-colls=mydb.testcollection--mongodb.uri=mongodb://147.96.81.119:30159 --discovering-mode --mongodb.direct-connect --compatible-mode --enable.dbstats --mongodb.collstats-colls=mydb.testcollection

podman ps -a
podman list
podman ps
sudo curl http://localhost:9216/metrics
ls
cat prometheus-config.yaml 
cd prometheus/
ls
cat prometheus_config.yml 
cd ..
ls
cd prometheus/
nano prometheus_config.yml 
cd ..
ls
cd mongodb-exporter/
la
ls
cd ..
cd mongodb-exporter-0.37.0.linux-amd64/
cd mongodb_exporter-0.37.0.linux-amd64/
ls
cat mongodb_exporter.yaml
cd ..
cat dashboard-ingress.yaml 
kubectl cluster-info dump | grep -m 1 cluster-cidr
kubectl cluster-info dump | grep -m 1 service-cluster-ip-range
kubectl get namespaces
kubectl delete namespace mongodb

kubectl delete namespace openvpn
ls 
rm mongodb-exporter
git clone https://github.com/k4kratik/k8s-openvpn.git
cd k8s-openvpn/deploy/openvpn/
cd ..
make-cadir ~/openvpn-ca
cd ~/openvpn-ca
source vars
./clean-all
./build-ca
./clean-all
source varsç
source vars
./build-ca
ls
cd easyrsa
./build-key-server server
sudo apt update
sudo apt install openvpn easy-rsa
make-cadir ~/openvpn-ca
cd ~/openvpn-ca
./easyrsa init-pki
./easyrsa build-server-full server nopass
run build-ca
./easyrsa build-ca
./easyrsa build-server-full server nopass
ls
cd pki
ls
cd private
ls
cd ..
./easyrsa build-server-full server nopass
cd ..
cd openvpn-ca
ls
./easyrsa build-server-full server nopass
./easyrsa init-pki
./easyrsa build-ca
./easyrsa build-server-full server nopass
./easyrsa gen-dh
sudo cp pki/ca.crt pki/private/ca.key pki/issued/server.crt pki/private/server.key pki/dh.pem /etc/openvpn
cd /etc/openvpn/
ls
rm server.conf
sudo nano server.conf
sudo rm server.conf
nano server.conf
sudo nano server.conf
sudo systemctl start openvpn@server
journalctl -xeu openvpn@server.service
ls
sudo nano server.conf
sudo systemctl start openvpn@server
sudo systemctl status openvpn@server
cd ~
cd easy-rsa/
ls
./easyrsa build-client-full estudianteclient nopass
cd ~/openvpn-ca
ls
./easyrsa build-client-full estudianteclient nopass
ls
cd pki
ls
sudo systemctl status openvpn@server
cd /etc/openvpn/
ls
sudo nano server.conf
systemctl reload openvpn@server
systemctl restart openvpn@server
sudo systemctl status openvpn@server
sudo journalctl -u mongodb_exporter.service
journalctl -xeu openvpn@server.service
sudo nano server.conf
journalctl -xeu openvpn@server.service
openvpn --genkey --secret ta.key
sudo openvpn --genkey --secret ta.key
ls
sudo nano server.conf
cat ta.key
journalctl -xeu openvpn@server.service
systemctl restart openvpn@server
systemctl status openvpn@server
sudo nano server.conf
journalctl -xeu openvpn@server.service
ls
ls -la
sudo chmod 777 ca.crt
sudo chmod 777 ca.key
sudo chmod 777 server.crt
sudo chmod 777 server.key
sudo chmod 777 ta.key
cat /etc/openvpn/server.conf
sudo iptables -A INPUT -p tcp --dport 110 -j ACCEPT
sudo netstat -tuln | grep 110
etc ..
cd ..
rm --help
rm -r openvpn/
SUDO rm -r openvpn/
sudo rm -r openvpn/
ls
cd ..
wget https://git.io/vpn -O openvpn-install.sh && bash openvpn-install.sh
cd home/estudiante
rm -r openvpn-pvc.yaml
rm -r openvpn-ca/
wget https://git.io/vpn -O openvpn-install.sh && bash openvpn-install.sh
sudo ./openvpn-install.sh
ls
sudo openvpn-install.sh
sudo bash openvpn-install.sh
cd /root/
sudo cd /root/
ls
cd ..
ls
cd root
sudo cd root
exit
cd
cd ..
sudo su
ls
cd home
ce estudiante/
cd estudiante/
ls
cat /etc/openvpn/server.conf
journalctl -xeu openvpn@server.service
systemctl status openvpn@server
systemctl stop openvpn@server
bash openvpn-install.sh
sudo bash openvpn-install.sh
chmod 777 estudiante.ovpn 
sudo chmod 777 estudiante.ovpn 
bash openvpn-install.sh
sudo bash openvpn-install.sh
curl -O https://raw.githubusercontent.com/angristan/openvpn-install/master/openvpn-install.sh
chmod +x openvpn-install.sh
./openvpn-install.sh
sudo ./openvpn-install.sh
systemctl status openvpn@.service.
systemctl status openvpn@.service
systemctl status openvpn@.service.
systemctl start openvpn@.service.
journalctl -xeu openvpn@.service..service
systemctl status openvpn@.service.
cd /etc/openvpn/
ls
nano server.conf
sudo ls /etc/openvpn/server/
sudo journalctl -u openvpn@server.service -e
sudo systemctl status openvpn@server.service
ls
cd client
ls
cd /home/estudiante
ls
chmod 777 alex.ovpn
sudo chmod 777 alex.ovpn
sudo journalctl -u openvpn@server.service -e
systemctl start openvpn@.service.
systemctl start openvpn@.service
systemctl stop  openvpn@.service
systemctl stop openvpn@.service.
sudo journalctl -u openvpn@server.service -e
journalctl -xeu openvpn@.service..service
journalctl -xeu openvpn@server.service
systemctl start  openvpn@.service..
journalctl -xeu openvpn@.service...service
systemctl start openvpn@.service
systemctl start openvpn@service
systemctl status openvpn@.service
systemctl status openvpn@service
systemctl status openvpn@service.
systemctl status openvpn@.service.
systemctl status openvpn@.service..
sudo systemctl stop openvpn@*.service
sudo systemctl disable openvpn@*.service
sudo apt purge openvpn
sudo rm -rf /etc/openvpn/
sudo rm /var/log/openvpn.log

rm -r easy-rsa
rm -r estudiante.ovpn
sudo ./openvpn-install.sh
sudo chmod 777 dani.ovpn.
sudo chmod 777 dani.ovpn
systemctl status openvpn
systemctl start openvpn
systemctl status openvpn
sudo systemctl status openvpn@server
journalctl -xeu openvpn@server
kubctl get pods -n mongo-sharded
kubectl get pods -n mongo-sharded
ping 147.96.81.119
podman ps
podman ps -a
kubectl get services
kubectl get services -n mongo
kubectl get all
kubectl get all --all-namespaces
kubectl get services -n mongo-sharded
exit
podman logs feec5ef2fccd
sudo curl http://localhost:9216/metrics
lsç

cd mongo
ls
kubectl apply -f mongodb-exporter-deployment.yaml -n mongo-sharded
kubectl get pods -n mongo-sharded
sudo curl http://localhost:9216/metrics
ls
kubectl port-forward pod/mongodb-exporter-deployment-5698d8b866-7jp92 9216:9216 -n mongo-sharded
kubectl get pods -n mongo-sharded
kubectl describe pod mongodb-exporter-deployment-5698d8b866-7jp92 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5698d8b866-7jp92 -n mongo-sharded
kubectl apply -f mongodb-exporter-deployment.yaml -n mongo-sharded
ls
kubectl get pods -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded -w
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
kubectl describe pod mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
kubectl describe pod mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
sudo curl http://localhost:9216/metrics
kubectl logs mongodb-exporter-deployment-5d466f7cd6-gf559 -n mongo-sharded
sudo curl http://localhost:9216/metrics
kubectl apply -f mongodb-exporter-deployment.yaml -n mongo-sharded
kubectl get pods -n mongo-sharded
kubectl logs mongodb-exporter-deployment-74dcb4f9bf-64cqq -n mongo-sharded
sudo curl http://localhost:9216/metrics
sudo curl http://147.96.81.119:9216/metrics
kubectl applt -f mongo-exporter-service.yaml 
kubectl apply -f mongo-exporter-service.yaml 
cd ..
cd prometheus/
ls
nano prometheus_config.yml
systemctl status prometheus
cat /lib/systemd/system/prometheus.service
cd /etc/default/prometheus
cd /usr/bin/
ls
nano prometheus
cd ..
cat /lib/systemd/system/prometheus.service
sudo systemctl daemon-reload
sudo systemctl restart prometheus
sudo curl http://localhost:9216/metrics
sudo curl http://localhost:9090/metrics
systemctl status prometheus
kubectl get pods -n mongo-sharded
cd /home/estudiante
ls
nano prometheus-config.yaml
kubectl apply -f prometheus-config.yaml -n test-ns
sudo curl http://localhost:9216/metrics
sudo curl http://localhost:9090/metrics
sudo curl http://localhost:9216/metrics
sudo systemctl stop mongodb_exporter.service
sudo curl http://localhost:9216/metrics
sudo systemctl reload prometheus
sudo curl http://localhost:9216/metrics
sudo curl http://localhost:9090
kubectl port-forward pod/mongodb-exporter-deployment-5698d8b866-7jp92 9216:9216 -n mongo-sharded
kubectl get pdos -n mongo-sharded
kubectl get pods -n mongo-sharded
kubectl port-forward pod/mongodb-exporter-deployment-74dcb4f9bf-64cqq 9216:9216 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-5698d8b866-7jp92 -n mongo-sharded
kubectl logs mongodb-exporter-deployment-74dcb4f9bf-64cqq -n mongo-sharded
kubectl status mongodb-exporter
systemctl status mongodb-exporter
~/go/bin/prometheus --config.file=prometheus_config.yml
cd prometheus
sudo prometheus --config.file=prometheus_config.yml
sudo netstat -tuln | grep 9216
sudo systemctl status prometheus
curl http://localhost:9216/metrics
kubectl descibe pod  mongodb-exporter-deployment-5698d8b866-7jp92 -n mongo-sharded
kubectl describe pod  mongodb-exporter-deployment-5698d8b866-7jp92 -n mongo-sharded
kubectl describe pods -n mongo-sharded
kubectl describe svc -n mongo-sharded
sudo prometheus --config.file=prometheus_config.yml
kill -HUP <PROMETHEUS_PID>
sudo systemctl reload prometheus
sudo prometheus --config.file=prometheus_config.yml
kill -HUP <PROMETHEUS_PID>
curl http://localhost:9216/metrics
curl http://10.244.3.47:9216/metrics
curl http://10.102.146.179:9216/metrics
sudo prometheus --config.file=prometheus_config.yml
systemctl show -p MainPID prometheus
kill -HUP 3676410
sudo kill -HUP 3676410
sudo prometheus --config.file=prometheus_config.yml
curl http://10.102.146.179:9216/metrics
10.102.146.179:9216/metrics
systemctl show -p MainPID prometheus
sudo systemctl reload prometheus
curl -X POST http://localhost:9090/-/reload
curl http://10.102.146.179:9216/metrics
kubectl apply -f prometheus_config.yml
kubectl apply -f prometheus_config.yml -n test-ns
nano prometheus
prometheus --config.file=/ruta/al/prometheus.yml --check-config
prometheus --config.file=/ruta/al/prometheus_config.yml --check-config
prometheus --config.file=/ruta/al/prometheus_config.yml 
prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml 
sudo prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml 
sudo systemctl stop prometheus
sudo prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml 
cat /etc/systemd/system/prometheus.service
cat /lib/systemd/system/prometheus.service
sudo nano /etc/default/prometheus
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo journalctl -u prometheus -e
/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml --check-config
sudo systemctl daemon-reload
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo journalctl -u prometheus -e
sudo -u prometheus /usr/bin/prometheus $ARGS
sudo journalctl -u prometheus.service
sudo systemctl status prometheus
/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml --check-config
sudo nano /etc/default/prometheus
ExecStart=/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml
ExecStart=/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo nano /etc/default/prometheus
sudo -u prometheus cat /home/estudiante/prom
sudo -u prometheus cat /home/estudiante/prometheus/prometheus_config.yml
sudo chown prometheus:prometheus /home/estudiante/prometheus/prometheus_config.yml
mv  /home/estudiante/prometheus/prometheus_config.yml /etc/default/
sudo mv /home/estudiante/prometheus/prometheus_config.yml /etc/default/
sudo su mv /home/estudiante/prometheus/prometheus_config.yml /etc/default/
sudo su mv /home/estudiante/prometheus/prometheus_config.yml /etc/defaultsudo systemctl restart prometheus

sudo su
sudo systemctl status prometheus
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo journalctl -u prometheus.serviceExecStart=/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml

ExecStart=/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yml
ExecStart=/usr/bin/prometheus --config.file=./prometheus_config.yml
ExecStart=/usr/bin/prometheus --config.file=prometheus_config.yml
sudo nano /etc/default/prometheus
mv prometheus_config.yml /etc/default
mv ./prometheus_config.yml /etc/default
ls
sumv ./prometheus_config.yaml /etc/default
sudo chown prometheus:prometheus /home/estudiante/prometheus/prometheus_config.yaml
sudo chmod 640 /home/estudiante/prometheus/prometheus_config.yaml
sudo nano /etc/default/prometheus
ExecStart=/usr/bin/prometheus --config.file=prometheus_config.yaml
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo -u prometheus /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml --help | grep config
sudo systemctl daemon-reload
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo -u prometheus /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo sudo -u prometheus /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo su
sudo chown -R prometheus:prometheus /home/estudiante/prometheus/
sudo chmod 640 /home/estudiante/prometheus/prometheus_config.yaml
sudo sudo -u prometheus /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo chmod -R 755 /home/estudiante/prometheus/
sudo chown prometheus:prometheus /home/estudiante/prometheus/prometheus_config.yaml
sudo setenforce 0
sudo -u prometheus cat /home/estudiante/prometheus/prometheus_config.yaml
sudo  cat /home/estudiante/prometheus/prometheus_config.yaml
sudo systemctl status prometheus
sudo prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo nano /lib/systemd/system/prometheus.service
sudo systemctl daemon-reload
sudo systemctl start prometheus
sudo systemctl enable prometheus
sudo systemctl status prometheus
sudo nano /lib/systemd/system/prometheus.service
sudo journalctl -u prometheus -xe
sudo nano /lib/systemd/system/prometheus.service
sudo systemctl daemon-reload
sudo systemctl start prometheus
sudo systemctl status prometheus
sudo journalctl -u prometheus -xe
sudo chmod 644 /home/estudiante/prometheus/prometheus_config.yaml
ls -l /home/estudiante/prometheus/prometheus_config.yaml
/usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo systemctl status prometheus
sudo /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo nano /lib/systemd/system/prometheus.service
curl http://localhost:9216/metrics
sudo systemctl start prometheus
sudo systemctl status prometheus
sudo systemctl start prometheus
sudo systemctl status prometheus
sudo /usr/bin/prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
sudo lsof -i :9090
curl http://localhost:10254/metrics
ls
nano prometheus-config.yaml
systemd
sudo systemctl status prometheusç
sudo systemctl status prometheus
sudo mkdir -p /var/lib/prometheus/metrics2/
sudo chown -R estudiante:estudiante /var/lib/prometheus/metrics2/
sudo systemctl restart prometheus
sudo systemctl status prometheus
sudo chown -R estudiante:estudiante /ruta/del/directorio
sudo systemctl start prometheus
sudo systemctl status prometheus
sudo journalctl -u prometheus -xe
sudo systemctl status prometheus.service
sudo journalctl -u prometheus.service
prometheus --config.file=/ruta/a/prometheus.yml --check-config
prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
kubectl get pods test-ns
kubectl get pods -n test-ns
sudo lsof -i :9090
sudo kill -9 [PID]
sudo kill -9 3712709
prometheus --config.file=/home/estudiante/prometheus/prometheus_config.yaml
kubectl get pods test-ns
kubectl get pods -n test-ns
kubectl delete namespace test-ns
kubectl create namespace monitoring
cd metricsConfiguration/
ls
kubectl apply -k . -n monitoring
kubectl apply -f prometheus-pvc-rook.yaml
ls
kubectl apply -f prometheus-pvc_rook.yaml
kubectl apply -f prometheus-cluster-role.yaml
kubectl apply -f prometheus-config.yaml
kubectl apply -f prometheus-deployment.yaml
kubectl get pods -monitoring
kubectl get pods -n monitoring
kubectl describe  pods -n monitoring
kubectl logs prometheus-659ff7f4bb-rtxbn -n monitoring
kubectl describe  pods -n monitoring
kubectl logs prometheus-659ff7f4bb-rtxbn -n monitoring
kubectl apply -f prometheus-deployment.yaml
kubectl logs prometheus-659ff7f4bb-rtxbn -n monitoring
kubectl describe  pods -n monitoring
kubectl logs prometheus-699b8c8ff-96vgc -n monitoring
kubectl describe  pods -n monitoring
kubectl get pods -n monitoring
kubectl aplly -f prometheus-service.yaml
kubectl apply -f prometheus-service.yaml
kubectl get svc -n monitoring
kubectl get svc -n mongo-sharded
kubectl get svc -n spark-operatorhelm upgrade [RELEASE_NAME] spark-operator/spark-operator --namespace [NAMESPACE] --set metrics.enable=true

kubectl get pods -n spark-operator
helm upgrade my-release spark-operator/spark-operator --namespace spark-operator --set metrics.enable=true
kubectl get pods -n spark-operator
kubectl port-forward my-release-spark-operator-85bbc7865-g7g2l 10254:10254 -n spark-operator
ls
podman ps -a
cd spark-on-k8s
ls
cd spark-3.4.1-bin-hadoop3/
ls
cd data
ls
cd mcd ..
cd ..
cd conf/
ls
cd ..
mkdir prometheus
docker login
docker build -t mongoSparkMonitoring .
docker build -t mongo-spark-monitoring .
ls
cd kubernetes/
ls
cd dockerfiles
ls
cd spark
ls
docker build -t mongo-spark-monitoring .
cd ..
cd .
cd ..
cd .
cd ..
docker build -t mongo-spark-monitoring ./kubernetes/dockerfiles
docker build -t mongo-spark-monitoring 
docker build -t mongo-spark-monitoring ./kubernetes/dockerfiles/
docker build -t mongo-spark-monitoring -f kubernetes/dockerfiles/spark/Dockerfile .
docker tag localhost/mongo-spark-monitoring:latest estudianteucm22/mongo-spark-monitoring:latest
docker push estudianteucm22/mongo-spark-monitoring:latest
kubectl create namespace spark-jobs
helm list
helm list -n spark-operator
helm upgrade my-release spark-operator/spark-operator --set sparkJobNamespace=spark-jobs -n spark-operator
cd ..
cd monitoring/
ls
kubectl apply spark-service.yaml 
kubectl apply -f spark-service.yaml 
kubectl get svc -n spark-jobs
cd ..
cd metricsConfiguration/
kubectl apply -f prometheus-config.yaml 
cd ..
cd monitoring/
ls
kubectl apply -f spark-role.yaml
kubectl apply -f spark-rolebinding.yaml
kubectl apply spark-pi-schedule.yaml 
kubectl apply -f spark-pi-schedule.yaml 
kubectl get sparkapplications -n spark-jobs
kubectl get sparkapplications
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pods -n spark-operator
kubectl logs my-release-spark-operator-8489cc97d5-4tmw5 -n spark-operator
kubectl get sparkapplications -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692358381324780084 -n spark-jobs
kubectl get serviceaccount spark -n spark-jobs
kubectl apply -f spark-role.yaml -n spark-jobs
kubectl apply -f spark-rolebinding.yaml -n spark-jobs
kubectl apply -f spark-sa.yaml
kubectl create rolebinding spark-my-role-binding   --role=spark-role   --serviceaccount=spark-jobs:spark   --namespace=spark-jobs
kubectl create clusterrolebinding spark-my-cluster-role-binding   --clusterrole=spark-rolebinding   --serviceaccount=spark-jobs:spark
kubectl logs my-release-spark-operator-8489cc97d5-4tmw5 -n spark-operator
kubectl delete  spark-pi-schedule.yaml 
kubectl delete spark-pi-schedule.yaml -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl logs my-release-spark-operator-8489cc97d5-4tmw5 -n spark-operator
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692358381324780084 -n spark-jobs
kubectl get sparkapplications.sparkoperator.k8s.io  -n spark-jobs
kubectl describe s -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl logs my-release-spark-operator-8489cc97d5-4tmw5 -n spark-operator
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692358381324780084 -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl logs spark-pi-scheduled-1692359281344799553 -n spark-operator
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl logs sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl logs spark-pi-scheduled-1692359281344799553-driver -n spark-jobs
kubectl delete job spark-pi-scheduled -n spark-jobs
kubectl get jobs -n spark-jobs
kubectl get jobs -n spark-operator
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl apply spark-pi-schedule.yaml
kubectl apply -f spark-pi-schedule.yaml
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl logs spark-pi-scheduled-1692359281344799553-driver -n spark-jobs
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl logs sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io pods-n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io pods -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359881357122341 -n spark-jobs
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl logs sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359881357122341-driver -n spark-jobs
kubectl logs spark-pi-scheduled-1692359881357122341-driver -n spark-jobs
kubectl exec -n spark-jobs spark-pi-scheduled-1692359881357122341-driver -- ls /jars
kubectl get pods -n spark-jobs -l spark-role=driver
kubectl exec -n spark-jobs spark-pi-scheduled-1692360181363027986-driver -- ls /jars
kubectl run debug-pod -n spark-jobs --image=docker.io/estudianteucm22/mongo-spark-monitoring -- /bin/sh
kubectl exec -it -n spark-jobs debug-pod -- /bin/sh
kubectl run debug-pod -n spark-jobs --image=docker.io/estudianteucm22/mongo-spark-monitoring -- /bin/sh
kubectl exec -it -n spark-jobs debug-pod -- /bin/sh
kubectl delete pod debug-pod -n spark-jobs
kubectl run debug-pod -n spark-jobs --image=docker.io/estudianteucm22/mongo-spark-monitoring -- /bin/sh
kubectl exec -it -n spark-jobs debug-pod -- /bin/sh
kubectl delete pod debug-pod -n spark-jobs
kubectl exec -it -n spark-jobs debug-pod -- /bin/sh
kubectl run debug --image=docker.io/estudianteucm22/mongo-spark-monitoring -- /bin/sh
kubectl exec -it debug -- /bin/sh
kubectl get pods
kubectl logs  pods
kubectl logs debug
kubectl get pods
kubectl logs debug
kubectl exec -it debug -- /bin/sh
kubectl get pod debug -n spark-jobs -o=jsonpath='{.spec.containers[*].name}'
kubectl get pod debug -n spark-jobs
kubectl get pods
kubectl logs debug
kubectl describe sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692359281344799553 -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692361081383730508-driver -n spark-jobs
exit
cd monitoring/
ls
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe pods -n spark-jobs
kubectl get  pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692367381521895203-driver -n spark-jobs
kubectl exec -n spark-jobs spark-pi-scheduled-1692367381521895203-driver -- find / -name "*.jar"
kubectl apply -f spark-pi-schedule.yaml
kubectl get  pods -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692368581549395294-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692368881556712940-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs

kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692370081582062618-driver -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692370681595748030-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl logs spark-pi-scheduled-1692372181624922415-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl logs spark-pi-scheduled-1692372181624922415-driver -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl get sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692365581478734919 -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692365881484729708 -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692366481500614713 -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692366781507979228 -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692367081515374628 -n spark-jobs
kubectl delete sparkapplications.sparkoperator.k8s.io spark-pi-scheduled-1692372481630668557 -n spark-jobs
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl get svc -n monitoring
kubectl get svc -n sharded-cluster
kubectl get svc -n mongo-sharded
kubectl get svc -n monitoring
kubectl get pods -n monitoring
ls
cd ..
cd metricsConfiguration/
ls
kubectl apply -f prometheus-config.yaml
kubectl apply -f prometheus-deployment.yaml
kubectl apply -f prometheus-service.yaml
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
kubectl get svc -n monitoring
kubectl get svc -n spark-jobs
kubectl apply -f prometheus-config.yaml
kubectl rollout restart deployment prometheus-deployment.yaml
kubectl get  deployment -n monitoring
kubectl rollout restart deployment prometheus -n monitoring
kubectl apply -f prometheus-config.yaml
kubectl rollout restart deployment prometheus -n monitoring
kubectl apply -f prometheus-config.yaml
kubectl rollout restart deployment prometheus -n monitoring
kubectl get  deployment -n monitoring
kubectl apply -f prometheus-config.yaml
kubectl rollout restart deployment prometheus -n monitoring
kubectl get  deployment -n monitoring
kubectl describe sparkapplications.sparkoperator.k8s.io -n spark-jobs
cd ..
cd monitoring/
ls
kubectl get  deployment -n monitoring
kubectl rollout restart deployment prometheus -n monitoring
kubectl get  deployment -n monitoring
kubectl svc -n spark-jobs
kubectl get svc -n spark-jobs
kubectl delete svc spark-service -n spark-jobs
kubectl get svc -n spark-jobs
cd ..kubectl apply -f prometheus-config.yaml -n monitoring
cd ..
cd metricsConfiguration
ls
kubectl apply -f prometheus-config.yaml
kubectl rollout restart deployment prometheus -n monitoring
kubectl get pods -n spark-jobs
kubectl run busybox-test --namespace=spark-jobs --image=busybox --rm -ti --restart=Never -- /bin/sh
kubectl get pods -n spark-jobs -l app=spark-pi-metrics  # asumimos que 'app' es la etiqueta correcta, ajusta según tu configuración.
kubectl describe svc spark-pi-metrics -n spark-jobs
kubectl get pods -n spark-jobs -l app=spark-pi-metrics  # asumimos que 'app' es la etiqueta correcta, ajusta según tu configuración.
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692375151692037354-driver -n spark-jobs
kubectl describe svc spark-pi-metrics -n spark-jobs
curl http://spark-pi-metrics.spark-jobs:8090/metrics
curl http://10.107.113.24:8090/metrics
kubectl describe svc spark-pi-metrics -n spark-jobs
kubectl logs spark-pi-scheduled-1692375151692037354-driver -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692376111709252102-driver -n sparl-jobs
kubectl logs spark-pi-scheduled-1692376111709252102-driver -n spark-jobs
kubectl get servicemonitor
cd ..
cd monitoring/
kubectl apply -f prometheus/servicemonitor-spark.yaml
kubectl apply -f servicemonitor-spark.yaml
kubectl describe service spark-service
kubectl describe service spark-jobs
kubectl get svc spark-jobs
kubectl get svc -n spark-jobs
kubectl describe svc spark-pi-metrics -n spark-jobs
kubectl get pods -n spark-jobs -l app=spark-pi-scheduled
kubectl describe pods -n spark-jobs 
ls
kubectl apply -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs -l app=spark-pi-scheduled
kubectl describe svc spark-pi-metrics -n spark-jobs
kubectl get pods -n spark-jobs -l app=spark-pi-scheduled
kubectl describe svc spark-pi-metrics -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml 
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl get pods -n spark-jobs -w
kubectl logs spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobsç
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl logs spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl logs spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl exec -it spark-history-server-5f574b97cd-7v82z -n spark-jobs -- /bin/sh
kubectl exec -it spark-history-server-5f574b97cd-7v82z -n spark-jobs -- /bin/sh -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl logs spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl describe pod spark-history-server-5f574b97cd-7v82z -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-759d77c774-9wb5n -n spark-jobs
kubectl logs spark-history-server-759d77c774-9wb5n -n spark-jobs
kubectl describe pod spark-history-server-759d77c774-9wb5n -n spark-jobs
kubectl logs spark-history-server-759d77c774-9wb5n -n spark-jobs
kubect get pvc -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl describe pod spark-history-server-759d77c774-9wb5n -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-84d447485f-26fgt -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-77746b549b-ztm9l -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-77746b549b-ztm9l -n spark-jobs
kubectl logs spark-history-server-77746b549b-ztm9l -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl describe pod spark-history-server-77746b549b-ztm9l -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-77746b549b-ztm9l -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl describe pod spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
kubectl apply -f spark-history-server-deployment.yaml 
kubectl logs spark-history-server-6766f8dc65-nzjgh -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl logs spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl exec -it spark-history-server-7dd54949f-w78gm -n spark-jobs -- cat /opt/spark/logs/spark--org.apache.spark.deploy.history.HistoryServer-1-spark-history-server-7dd54949f-w78gm.out
kubectl describe pod spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
kubectl apply -f spark-history-server-deployment.yaml 
kubectl describe pod spark-history-server-7dd54949f-w78gm -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-85bdcdc56c-9tp6g -n spark-jobs
kubectl describe pod spark-history-server-85bdcdc56c-9tp6g -n spark-jobs
kubectl logs spark-history-server-85bdcdc56c-9tp6g -n spark-jobs
kubectl describe pod spark-history-server-85bdcdc56c-9tp6g -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
helm show values bitnami/spark > values.yaml
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm show values bitnami/spark > values.yaml
helm install spark-history-server bitnami/spark -f values.yaml --namespace spark-jobs
kubectl get pods -n spakr-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-master-0 -n spark-jobs
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
helm upgrade spark-history-server bitnami/spark -f values.yaml --namespace spark-jobs
kubectl get svc -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl get svc -n spark-jobs
nano pvc-values.yaml
helm upgrade spark-history-server bitnami/spark -f pvc-values.yaml --namespace spark-jobs
kubectl get pvc -n spark-jobs
kubectl get svc -n spark-jobs
nano pvc-values.yaml
kubectl get pvc -n spark-jobs
exit
kubectl get namespaces
kubectl config view --minify --output 'jsonpath={..namespace}'
kubectl config set-context --current --namespace=monitoring
helm repo add kubernetes-dashboard https://kubernetes.github.io/dashboard/
helm repo update
helm install kubernetes-dashboard kubernetes-dashboard/kubernetes-dashboard
kubectl get service kubernetes-dashboard -n kubernetes-dashboard
kubectl get service kubernetes-dashboard -n monitoring
kubectl edit service kubernetes-dashboard -n monitoring
helm uninstall kubernetes-dashboard kubernetes-dashboard/kubernetes-dashboard
helm uninstall kubernetes-dashboard kubernetes-dashboard/monitoring
helm uninstall kubernetes-dashboard monitoring/kubernetes-dashboard
helm uninstall kubernetes-dashboard -n monitoring
helm list -n monitoring
helm install kubernetes-dashboard kubernetes-dashboard/kubernetes-dashboard --set service.type=NodePort
kubectl get service kubernetes-dashboard -n monitoring
kubectl -n kubernetes-dashboard describe secret $(kubectl -n kubernetes-dashboard get secret | grep kubernetes-dashboard | awk '{print $1}')
kubectl -n monitoring describe secret $(kubectl -n kubernetes-dashboard get secret | grep kubernetes-dashboard | awk '{print $1}')
kubectl -n monitoring describe secret $(kubectl -n monitoring  get secret | grep kubernetes-dashboard | awk '{print $1}')
kubectl -n monitoring describe secret $(kubectl -n monitoring get secret | grep kubernetes-dashboard | awk '{print $1}') | grep "token:" | awk '{print $2}'
kubectl create token SERVICE_ACCOUNT_NAME -n monitoring
kubectl -n monitoring get secrets/admin-user-secret -o jsonpath="{.data.token}"
kubectl -n monitoring create token admin-user
kubectl config view --minify --output 'jsonpath={.clusters[0].cluster.certificate-authority}'
kubectl config view --minify --output 'jsonpath={.users[0].user.client-certificate}'
kubectl -n monitoring get secret $(kubectl -n monitoring get secret | grep kubernetes-dashboard | awk '{print $1}') -o jsonpath='{.data.token}' | base64 -d
nano dashboard-adminuser.yaml
kubectl apply -f dashboard-adminuser.yaml
kubectl -n monitoring create token admin-user
nano dashboard-cluster-bind.yaml
kubectl apply -f dashboard-cluster-bind.yaml 
exit
ls
cd monitoring
ls
kubectl get pvc
kubectl get pvc -n spark-jobs
kubectl delete pvc  -n spark-jobs
kubectl delete pvc pvc-spark-data -n spark-jobs
helm list
helm list
helm delete spark-history-server bitnami/spark  --namespace spark-jobs
helm delete spark-history-server --namespace spark-jobs
helm list
helm list -n spark-jobs
kubectl get pods -n spark jobs
kubectl get pods -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-history-server-d4d7cf6f5-jsnwr -n spark-jobs
kubectl logs  spark-history-server-d4d7cf6f5-jsnwr -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
kubectl get pvc -n spark-jobs
kubectl delete pvc spark-eventlogs-pvc -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl delete pvc spark-eventlogs-pvc -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml 
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl describe pod  -n spark-jobs
kubectl describe pod spark-history-server-74688c8c98-xz8fg -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-74688c8c98-xz8fg -n spark-jobs
kubectl logs spark-history-server-74688c8c98-xz8fg -n spark-jobs --previous
kubectl describe pod spark-history-server-74688c8c98-xz8fg -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-history-server-74688c8c98-xz8fg -n spark-jobs --previous
kubectl delete -f spark-history-server-deployment.yaml 
kubectl delete pvc spark-eventlogs-pvc -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml 
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl apply -f spark-history-server-service.yaml 
kubectl get svc -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl get svc -n spark-jobs
kubectl pods svc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692612066888165967-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl get pods -n spark-jobs
kubectl -n monitoring describe secret $(kubectl -n monitoring get secret | grep admin-user | awk '{print $1}')
kubectl -n monitoring create token admin-user --duration=8760h
exit
30458exit
exit
kubectl get svc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692619807061840519-drive -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692619807061840519-driver -n spark-jobs
kubectl describe pvc spark-eventlogs-pvc -n spark-jobs
kubectl describe pv pvc-34a3da7c-9f93-41cf-b7b5-436170a6581d
kubectl describe pods -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
cd monitoring/
kubectl apply -f spark-pi-schedule.yaml 
kubectl get pvc -n spark-jobs
helm list -n spark-operator
helm upgrade  my-release spark-operator/spark-operator --namespace spark-operator --set webhook.enable=true
kubectl describe pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692622581460521069-driver -n spark-jobs
kubectl logs spark-pi-scheduled-1692621081424807455-driver -n spark-jobs
kubectl logs spark-pi-scheduled-1692622581460521069-driver -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml 
kubectl get pods -n spark-jobs
kubectl get pods -n mongo
kubectl get pods -n mongo-sharded
kubectl exec -it mongodb-mongos-77cb5d8765-fsmgt -- mongo -n mongo-sharded
kubectl exec -it mongodb-mongos-77cb5d8765-mdgqc -- mongo -n mongo-sharded
kubectl get pods -n mongo-sharded
kubectl exec -it mongodb-mongos-77cb5d8765-mdgqc
kubectl exec -it mongodb-mongos-77cb5d8765-mdgqc -- mongo
kubectl exec -it -n mongo-sharded  mongodb-mongos-77cb5d8765-mdgqc -- mongo
kubectl get pods -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl get pods -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml 
kubectl get jobs -n spark-jobs
kubectl get pods -n spark-jobs
kubectl get pods -n spark-operator
kubectl logs my-release-spark-operator-549486cdd5-82wdn -n spark-operator
kubectl get pods -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692623181475701228-drive -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml 
cdc..
cd ..
cd rook/
ls
cd rook
ls
cd deploy/
ls
cd examples/
ls
kubectl apply -f filesystem.yaml 
ubectl -n rook-ceph exec -it deploy/rook-ceph-tools -- bash
kubectl -n rook-ceph exec -it deploy/rook-ceph-tools -- bash
cd ..
cd examples/
cd csi/
ls
cd cephfs/
kubectl apply -f storageclass.yaml 
kubectl get pvc
kubectl get pvc -n spark-jobs
cd
cd monitoring/
ls
kubectl delete spark-pvc-claim.yaml
kubectl delete spark-pvc-claim.yaml -n spark-jobs
kubectl delete spark-pvc-claim -n spark-jobs
kubectl get pvc
kubectl delete pvc -n spark-jobs
kubectl delete pvc spark-eventlogs-pvc -n spark-jobs
kubectl get pvc
kubectl get pvc -n spark-jobs
kubectl delete spark-history-server-service.yaml -n spark-jobs
kubectl delete spark-history-server-deployment.yaml -n spark-jobs
kubectl get pods
kubectl get pods -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl delete -f spark-pvc-claim.yaml -n spark-jobs
kubectl delete -f spark-history-server-service.yaml.yaml -n spark-jobs
kubectl delete -f spark-history-server-service.yaml -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml.yaml -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml -n spark-jobs
kubectl delete -f spark-pvc-claim.yaml -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl apply -f spark-history-server-service.yaml -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692625011516079967-driver -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe spark-pi-scheduled-1692625071517333546-driver -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe spark-pi-scheduled-1692625071517333546-driver -n spark-jobs
kubectl describe pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-history-server-service.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl apply -f spark-history-server-service.yaml -n spark-jobs
kubectl delete spark-pi-schedule.yaml -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl get serviceAccounts
kubectl get serviceAccounts -n spark-jobs
kubectl get roles -n spark-jobs
kubectl create -f spark-role.yaml -n spark-jobs
kubectl get roles -n spark-jobs
kubectl apply -f spark-rolebinding.yaml -n spark-jobs
kubectl get roleBinding -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs spark-pi-scheduled-1692625671532157391-driver -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod spark-pi-35e5ec8a185c312a-exec-1 -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl apply -f spark-history-server-service.yaml -n spark-jobs
kubectl delete -f spark-history-server-service.yaml -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml.yaml -n spark-jobs
kubectl delete -f spark-history-server-deployment.yaml -n spark-jobs
kubectl delete -f spark-pvc-claim.yaml -n spark-jobs
kubectl apply -f spark-pvc-claim.yaml -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl apply -f spark-history-server-deployment.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl apply -f spark-history-server-service.yaml -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml -n spark-jobs
kubectl apply -f spark-pi-schedule.yaml -n spark-jobs
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pvc -n spark-jobs
mongo
mongos
cd mongo
cat comanditos.txt
kubectl exec -it mongodb-mongos-77cb5d8765-fsmgt -- mongo
kubectl port-forward svc/mongodb-mongos 30159:30159
kubectl exec -it mongodb-mongos-77cb5d8765-fsmgt -- mongo -n mongo-sharded
ls
cat mongodb-exporter-deployment.yaml 
kubectl exec -it -n mongo-sharded mongodb-mongos-77cb5d8765-fsmgt -- mongo
kubectl exec -it -n mongo-sharded mongodb-mongos-77cb5d8765-fsmgt --mongo
kubectl exec -it -n mongo-sharded mongodb-mongos-77cb5d8765-fsmgt -- mongo
kubectl exec -it -n mongo-sharded mongodb-mongos-77cb5d8765-mdgqc -- mongo
sudo systemctl status mongodb_exporter.service
curl http://localhost:9216/metrics
curl http://localhost:30363/metrics
curl http://localhost:31363/metrics
sudo systemctl status mongodb_exporter.service
exit
sl
ls
exit
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
watch kubectl get pods -n argocd
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d && echo
kubectl get svc -n argocd
kubectl patch svc argocd-server -n argocd -p '{"spec": {"type": "NodePort"}}'
kubectl get svc -n argocd
cd ..
cd etc
ls
cd
ssh-keygen -t rsa -b 4096 -C "alejmo12@ucm.es"
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub
cat ~/.ssh/id_rsa
cd monitoring/
ls
kubectl get svc -n spark-jobs
kubectl get svc mongo
kubectl get svc mongo-sharded
kubectl get svc -n mongo-sharded
kubectl get services -n mongo-sharded
exit
ls
cd prometheus/
ls
cd .
cd ..
cat prometheus-config.yaml 
cd prometheus/
ls
cat prometheus_config.yaml 
cd .. 
ls
cd monitoring/
ls
cd ..
cd metricsConfiguration/
ls
cat kustomization.yaml 
cat prometheus-c
cat prometheus-config.yaml 
sudo systemctl status prometheus
kubectl get deploy
kubectl get deploy -n mongo-sharded
kubectl edit deploy grafana -n mongo-sharded
kubectl get serviceaccount -n argocd
kubectl patch deployment argocd-server -n argocd -p '{"spec": {"template": {"spec": {"serviceAccountName": "argocd-service-account"}}}}'
kubectl get deployment argocd-server -n argocd -o=jsonpath='{.spec.template.spec.serviceAccountName}'
kubectl get rolebindings -n spark-jobs
kubectl get clusterrolebindings -n argocd
kubectl describe clusterrole argocd-cluster-role
kubectl describe clusterrolebinding argocd-cluster-rolebinding
kubectl get svc argo-cd
kubectl get svc -n argo-cd
kubectl get pods -n argo-cd
kubectl get namespaces
kubectl get pods -n argoccd
kubectl get svc -n argocd
kubectl get svc -n prometheus
kubectl get svc -n monitoring
kubectl get svc -n mongo-sharded
kubectl get rolebinding,clusterrolebinding -n argocd
kubectl get rolebinding,clusterrolebinding -n argocd | argo-server
kubectl get rolebinding,clusterrolebinding -n argocd | argocd-server
kubectl get rolebinding,clusterrolebinding -n argocd grep | argocd-server
kubectl get rolebinding,clusterrolebinding -n argocd  | grep argocd-server
kubectl describe clusterrole argocd-server
kubectl config set-context --current --namespace=argocd
argocd 
brew install argocd
sudo apt install argocd
curl -sSL -o argocd-linux-amd64 https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64
sudo install -m 555 argocd-linux-amd64 /usr/local/bin/argocd
rm argocd-linux-amd64
argocd
argocd login localhost:31658
argocd app create spark-app --repo git@github.com:Mormur22/ArgoScripts.git --path spark-jobs --project spark --dest-server https://kubernetes.default.svc --dest-namespace spark-jobs --revision argocd --sync-policy auto
argocd app create spark-app --repo git@github.com:Mormur22/ArgoScripts.git --path spark-jobs --project spark --dest-server https://kubernetes.default.svc --dest-namespace default --revision argocd --sync-policy auto
argocd app create spark-app --repo git@github.com:Mormur22/ArgoScripts.git --path spark-jobs --project spark --dest-server https://kubernetes.default.svc --dest-namespace default --revision HEAD --sync-policy auto
argocd app create spark-app --repo git@github.com:Mormur22/ArgoScripts.git --path spark-jobs --project spark --dest-server https://kubernetes.default.svc --dest-namespace spark-jobs --revision HEAD --sync-policy auto
curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
helm repo add jupyterhub https://jupyterhub.github.io/helm-chart/
helm repo update
openssl rand -hex 32
ls
mkdir jupyter
cd jupyter/
helm upgrade --install jupyterhub jupyterhub/jupyterhub --namespace jhub --create-namespace --version=0.11.1 --values config.yaml
ls
helm list
helm list -n jhub
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/
helm repo update
helm upgrade --cleanup-on-fail   --install jupy-release jupyterhub/jupyterhub   --namespace jhub   --create-namespace   --version=3.0.2   --values config.yaml
kubectl completion bash
apt-get install bash-completion
sudo apt-get install bash-completion
echo 'source <(kubectl completion bash)' >>~/.bashrc
kubectl completion bash >/etc/bash_completion.d/kubectl
sudo kubectl completion bash >/etc/bash_completion.d/kubectl
sudo su
k -
kubectl -
kubectl --namespace=jhub get pod
kubectl --namespace=jhub describe pod hub-7bbf9456b9-587hm
kubectl apply -f jupyther-pvc-claim.yaml 
helm upgrade jupyterhub jupyterhub/jupyterhub --namespace <tu-namespace> -f config.yaml
helm upgrade jupyhub jupyterhub/jupyterhub --namespace jhub -f config.yaml
helm upgrade jupyterhub jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl --namespace=jhub get pod
helm list -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl --namespace=jhub get pod
kubectl --namespace=jhub describe pod hub-bc884bdcd-7smwb
kubectl apply -f hub-pvc-claim.yaml
kubectl --namespace=jhub get pod
kubectl describe pvc hub-db-dir
kubectl describe pvc hub-db-dir -n jhub
kubectl edit pvc hub-db-dir -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl describe pvc hub-db-dir -n jhub
kubectl --namespace=jhub get pod
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl --namespace=jhub get svc
ls
spark submit
spark-submit
cd ..
cd spark-on-k8s
ls
cd spark-3.4.1-bin-hadoop3/
ls
cd
helm repo add jahstreet https://jahstreet.github.io/helm-charts
helm repo update
kubectl config
kubectl get config
kubectl get serviceaccount -n spark-jobs
cd livy/
ls
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
cd argocd/
kubectl apply argo-sa.yaml 
kubectl apply -f argo-sa.yaml 
kubectl apply -f argo-role.yaml
kubectl apply -f argo-role-binding.yaml 
kubectl edit deployment argocd-server -n argocd
cd ..
cd jupyter/
docker build -t jupyter-mongo-spark:latest
docker build -t jupyter-mongo-spark:latest .
docker login
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:latest
docker push estudianteucm22/jupyter-mongo-spark:latest
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
docker build -t jupyter-mongo-spark:latest .
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:latest
docker push estudianteucm22/jupyter-mongo-spark:latest
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
docker list
docker 
podman
podman --help
podman search
docker image list
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:latest
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:0.0.1
docker build -t jupyter-mongo-spark:0.0.1 .
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:0.0.1
docker build -t jupyter-mongo-spark:latest
docker build -t jupyter-mongo-spark:latest .
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:latest
docker push estudianteucm22/jupyter-mongo-spark:latest
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
docker build -t jupyter-mongo-spark:latest .
docker tag jupyter-mongo-spark:latest estudianteucm22/jupyter-mongo-spark:latest
docker push estudianteucm22/jupyter-mongo-spark:latest
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
helm list -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
cd jupyter/
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
cd ..
ls
cat k8s-server-cert.pem | base64
cd jupyter/
kubectl apply -f spark-secret.yaml 
cd ..
cat k8s-server-cert.pem | base64
cd jupyter/
kubectl apply -f spark-secret.yaml 
kubectl apply -f spark-secret.yaml -n spark-jobs
kubectl get secrets -n spark-jobs
kubectl config view
APISERVER=$(kubectl config view --minify | grep server | cut -f 2- -d ":" | tr -d " ")
TOKEN=$(kubectl describe secret default-token | grep -E '^token' | cut -f2 -d':' | tr -d " ")
curl $APISERVER/api --header "Authorization: Bearer $TOKEN" --insecure
kubectl describe secret default-token
kubectl cluster-info
openssl s_client -connect master:443 -showcerts < /dev/null | openssl x509 -outform PEM > k8s-server-cert.pem
openssl s_client -connect 147.96.81.119:443 -showcerts < /dev/null | openssl x509 -outform PEM > k8s-server-cert.pem
ls
kubectl get pods -n juhb
kubectl get pods -n jhub
kubectl create configmap k8s-server-cert --from-file=k8s-server-cert.pem -n jhub
kubectl edit deployment jupyter-estudiante -n jhub
kubectl get deployments -n jhub
kubectl create configmap k8s-server-cert --from-file=k8s-server-cert.pem -n jhub
kubectl create secret generic k8s-server-cert --from-file=k8s-server-cert.pem -n jhub
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}'
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}' -n spark-jobs
kubectl ger serviceaccount 
kubectl get serviceaccount 
kubectl get serviceaccount -n default
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}' -n default
kubectl create clusterrolebinding spark-role --clusterrole=cluster-admin --serviceaccount=default:spark
kubectl create clusterrolebinding spark-role-admin --clusterrole=cluster-admin --serviceaccount=default:spark
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}'
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}' -n default
kubectl create clusterrolebinding spark-role-admin --clusterrole=cluster-admin --serviceaccount=default:spark -n default
kubectl get serviceaccount spark -o=jsonpath='{.secrets[0].name}' -n kube
kubectl get namespaces
kubectl describe serviceaccount spark
kubectl describe serviceaccount spark -n default
kubectl describe serviceaccount  -n spark-jobs
TOKEN=$(kubectl describe secret default-token | grep -E '^token' | cut -f2 -d':' | tr -d " ")
kubectl get secret
echo -n|openssl s_client -connect master:6446|openssl x509 -outform PEM > selfsigned_certificate.pem
echo -n|openssl s_client -connect 147.96.81.119:6446|openssl x509 -outform PEM > selfsigned_certificate.pem
cd jupyter/
cat k8s-server-cert.pem | base64
cd .
cd..
cd ..
cat k8s-server-cert.pem | base64
base64 -w 0 tu-certificado.pem
base64 -w 0 k8s-server-cert.pem
kubectl get secret mi-secret-cert -n spark-jobs -o yaml
cd livy
helm upgrade --install livy --namespace spark-jobs -f livy-conf.yaml 
helm upgrade --install livy --namespace spark-jobs jahstreet/livy -f livy-conf.yaml 
helm upgrade --install livy --namespace spark-jobs jahstreet/livy 
kubectl create configmap livy-conf-config-map --from-file=livy.conf
kubectl create configmap spark-defaults-conf-config-map --from-file=spark-defaults.conf
helm upgrade --install livy --namespace spark-jobs jahstreet/livy 
helm upgrade --install livy --namespace spark-jobs jahstreet/livy -f livy-conf.yaml 
kubectl --namespace spark-jobs get pods -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs describe  pods -l "app.kubernetes.io/instance=livy"
kubectl create configmap livy-conf-config-map --from-file=livy.conf -n spark-jobs
kubectl create configmap spark-defaults-conf-config-map --from-file=spark-defaults.conf -spark-jobs
kubectl create configmap spark-defaults-conf-config-map --from-file=spark-defaults.conf -n spark-jobs
kubectl --namespace spark-jobs get pods -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs describe pods -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs get pods -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs describe pods -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs get svc -l "app.kubernetes.io/instance=livy"
kubectl --namespace spark-jobs logs -l "app.kubernetes.io/instance=livy"
exit
kubectl get pods -n spark-jobs

kubectl cluster-info
openssl s_client -connect master:6443 </dev/null | openssl x509 -outform PEM > my_server_cert.pem
keytool -import -trustcacerts -file my_server_cert.pem -alias master -keystore $JAVA_HOME/jre/lib/security/cacerts
cat /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
cd /var/run/secrets/kubernetes.io/
kubectl get pods
kubectl get pods -n jhub
kubectl describe jupyter-estudiante -n jhub
kubectl describe pod jupyter-estudiante -n jhub
kubectl describe sericeaccounts -n jhub
kubectl describe serviceaccounts -n jhub
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=localhost/estudianteucm22/spark:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
cd spark-on-k8s
ls
cd spark-3.4.1-bin-hadoop3/
ls
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=localhost/estudianteucm22/spark:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./bin spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=localhost/estudianteucm22/spark:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
cd bin/
ls
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=localhost/estudianteucm22/spark:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=localhost/estudianteucm22/spark:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
clear
cd
kubectl get pods -n spark-jobs
kubectl get pods
kubectl get pods -n spark
kubectl get pods -n spark-jobs
kubectl proxy
kubectl config set-context --current --namespace=default
kubectl create serviceaccount spark-driver
kubectl create rolebinding spark-driver-rb --clusterrole=cluster-admin --serviceaccount=default:spark-driver
kubectl create serviceaccount spark-minion
kubectl create rolebinding spark-minion-rb --clusterrole=edit --serviceaccount=default:spark-minion
kubectl run spark-test-pod --generator=run-pod/v1 -it --rm=true   --image=apache/spark-py   --serviceaccount=spark-driver   --command -- /bin/bash
kubectl run spark-test -it --rm=true   --image=apache/spark-py   --serviceaccount=spark-driver   --command -- /bin/bash
kubectl run spark-test -it  --image=apache/spark-py   --serviceaccount=spark-driver   --command -- /bin/bash
kubectl get pods
kubectl logs spark-pi-scheduled-1692892437612762999-driver
kubectl get pods
javac InstallCert.java
kubectl get configmap -n kube-system extension-apiserver-authentication -o=jsonpath='{.data.client-ca-file}'
cat /etc/kubernetes/pki/apiserver.crt
docker run -it --entrypoint /bin/bash docker.io/estudianteucm22/spark:spark-autorized
keytool -list -cacerts -storepass changeit
docker run -it --entrypoint /bin/bash docker.io/estudianteucm22/spark:spark-autorized
podman -a
podman a
podman ps -a
docker run -it --entrypoint /bin/bash docker.io/estudianteucm22/spark:spark-autorized
kubectl get serviceaccounts
kubectl get serviceaccounts -n default
kubectl get serviceaccount spark -o yaml
cd spark-on-k8s
ls
cd spark-3.4.1-bin-hadoop3/
ls
cd bin
ls
docker login
docker-image-tool.sh -r estudianteucm22 -t spark-autorized build
./docker-image-tool.sh -r estudianteucm22 -t spark-autorized build
docker push estudianteucm22/spark:spark-autorized
./docker-image-tool.sh -r estudianteucm22 -t spark-autorized build
./docker-image-tool.sh -r estudianteucm22 -t spark-autorized build -p ../kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
docker push estudianteucm22/spark:spark-autorized
./docker-image-tool.sh -r estudianteucm22 -t spark-autorized build -p ../kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
docker push estudianteucm22/spark:spark-autorized
./docker-image-tool.sh -r estudianteucm22 -t spark-autorized build -p ../kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
docker push estudianteucm22/spark:spark-autorized
kubectl get all -n rook-ceph
kubectl get all -n rook-ceph | grep operator
kubectl get all -n rook-ceph | grep operator | awk '{printf "%-45s %-20s %-12s %-15s %-10s\n", $1, $2, $3, $4, $5}'
cd rook
ls
cd rook
ls
cd deploy/
ls
cd examples/
LS
ls
kubectl get -n rook-ceph jobs.batch
kubectl get pods -n rook-ceph -l app=rook-ceph-osd
kubectl -n rook-ceph get cephcluster
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod hub-6589d97bb5-r7qm4 -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod jupyter-estudiante -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod pyspark-shell-4b52878a33f29859-exec-1 -n spark-jobs
ls
kubectl get pods -n spark-jobs
kubectl get pvc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl describe pod pyspark-shell-4b52878a33f29859-exec-1 -n spark-jobs
kubectl get pods -n spark-jobs --field-selector=status.phase=Pendinf -o json | kubectl delete -f -
kubectl get pods -n spark-jobs --field-selector=status.phase=Pending -o json | kubectl delete -f -
kubectl get pods -n spark-jobs --field-selector=status.phase=Error -o json | kubectl delete -f -
kubectl get pods -n spark-jobs
kubectl logs pyspark-shell-1d4bdf8a33f8ce2c-exec-40 -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pods -n spark-jobs
kubectl get pods -n spark-jobs --field-selector=status.phase=Pending -o json | kubectl delete -f -
kubectl get pods -n spark-jobs
kubectl svc pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl svc pods -n spark-jobs
kubectl get pods -n spark-jobs
kubectl svc pods -n spark-jobs
kubectl get svc -n spark-jobs
kubectl get pods -n spark-jobs --field-selector=status.phase=Pending -o json | kubectl delete -f -
kubectl describe pod jupyter-estudiante -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs pyspark-shell-200d0a8a342323a0-exec-34 -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs pyspark-shell-200d0a8a342323a0-exec-34 -n spark-jobs
kubectl describe pod jupyter-estudiante -n spark-jobs
kubectl get svc -n spark-jobs
kubectl logs pyspark-shell-200d0a8a342323a0-exec-34 -n spark-jobs
kubectl get pods -n spark-jobs
kubectl logs pyspark-shell-4384e08a3425b8fa-exec-4 -n spark-jobs
kubectl get svc -n spark-jobs
kubectl proxy
./bin/spark-submit   --master k8s://https://kubernetes.default.svc:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=spark-operator   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
ls
cd spark-3.4.1-bin-hadoop3/
ls
./bin/spark-submit   --master k8s://https://kubernetes.default.svc:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=spark-operator   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
./bin/spark-submit   --master k8s://https://master:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=spark-operator   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
kubectl get pods -n spark-operator
kubectl logs spark-pi-9226f38a3303af36-driver -n spark-operator
./bin/spark-submit   --master k8s://https://147.96.81.119:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=spark-operator   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
./bin/spark-submit   --master k8s://https://147.96.81.119:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=jhub   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
cd ..
kubectl get configmap -o yaml jupyterhub-config
cd jupyter/
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl describe pods -n jhub
cd etc
cd /etc/
ls
cd
kubectl get pods -n jhub
kubectl delete pod jupyter-estudiante -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
cd jupyter/
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl delete pod jupyter-estudiante -n jhub
kubectl get pods -n jhub
kubectl logs hub-76df5678cd-6wp7w -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl get pods -n jhub
kubectl logs hub-76df5678cd-6wp7w -n jhub
kubectl logs hub-85777c9f99-gn7zv -n jhub
helm upgrade jupy-release jupyterhub/jupyterhub --namespace jhub -f config.yaml
kubectl get pods -n jhub
helm delete jupy-release jupyterhub/jupyterhub
helm delete jupy-release -n jhub
kubectl get pvc -n jhub
kubectl delete -f jupyther-pvc-claim.yaml -n jhub
kubectl  get pvcn jhub
kubectl  get pvc jhub
kubectl  get pvc -n jhub
kubectl delete -n jhub
kubectl all  --all -n jhub
kubectl delete all  --all -n jhub
kubectl delete namespace jhub
kubectl get serviceaccounts -n spark-jobs
kubectl apply -f hub-pvc-claim.yaml -n spark-jobs
kubectl apply -f jupyter-pvc-claim.yaml -n spark-jobs
kubectl apply -f jupyther-pvc-claim.yaml -n spark-jobs
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
helm install jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
helm uninstall jupy-release jupyterhub/jupyterhub
helm uninstall jupy-release jupyterhub/jupyterhub -n jhub
helm uninstall jupy-release jupyterhub/jupyterhub -n spark-jobs
kubectl delete -f hub-pvc-claim.yaml -n spark-jobs
helm install jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd ..
cd monitoring/
kubectl apply -f spark-rolebinding.yaml -n spark-jobs
cd jupyter/
ls
kubectl apply -f spark-token-manual.yaml
exit
kubectl edit deploy grafana -n mongo-sharded
kubectl delete  pod -n spark-jobs jupyter-estudiante 
kubectl get svc -n spark-jobs 
kubectl delete  pod -n spark-jobs jupyter-estudiante 
kubectl describe pod -n spark-jobs jupyter-estudiante 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-f0082f8a3cfe0eaf-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl get svc -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-0a2cc68a3d04cca7-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-0a2cc68a3d04cca7-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-0a2cc68a3d04cca7-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-a0a6888a3d0ab4a7-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-880df18a3d12c596-exec-1 -n spark-jobs 
kubectl delete  pod -n spark-jobs jupyter-estudiante 
kubectl get pods -n spark-jobs 
kubectl describe pod jupyter-estudiante
kubectl describe pod jupyter-estudiante -n spark-jobs 
kubectl get svc -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-9
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-19
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-17
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-16
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-16
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-19
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-21
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-19
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-21
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-22
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-23
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-21
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-22
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-35
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-35
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-30
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-40
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-44
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-47
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-50
kubectl get pods -n spark-jobs 
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-50
kubectl describe pod pyspark-shell-85cfda8a3d1cf003-exec-55
kubectl logs  pyspark-shell-85cfda8a3d1cf003-exec-55 -n spark-jobs
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-9422d58a3d214e75-exec-1 -n spark-jobs
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-9422d58a3d214e75-exec-1 -n spark-jobs
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl describe pods -n spark-jobs 
kubectl  describe pod  -n spark-jobs jupyter-estudiante
kubectl describe pods -n spark-jobs | grep 
kubectl get svc -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-60be438a3d27d92e-exec-1 -n spark-jobs 
kubectl delete pod -n spark-jobs jupyter-estudiante 
kubectl get pods -n spark-jobs 
kubectl delete pod spark-pi-scheduled-1692625911551395276-driver -n spark-jobs
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-0188e68a3d3a4ee1-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-0188e68a3d3a4ee1-exec-1 -n spark-jobs 
kubectl logs pyspark-shell-f4291e8a3d4163e7-exec-1 -n spark-jobs 
kubectl get svc -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-b1fb1b8a3d4d3645-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl get svc -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-ba90e28a3d8a3139-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-ba90e28a3d8a3139-exec-1 -n spark-jobs 
kubectl logs pyspark-shell-ba90e28a3d8a3139-exec-2 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs pyspark-shell-87f0b58a3d8e4f49-exec-1 -n spark-jobs 
kubectl delete pod -n spark-jobs jupyter-estudiante 
kubectl get svc mongo-sharded
kubectl get svc -n mongo-sharded
kubectl logs -n spark-jobs jupyter-estudiante 
kubectl get pods -n spark-jobs 
kubectl describe  pod pyspark-shell-02bc048a3dea5562-exec-1 -n spark-jobs 
kubectl get pods -n spark-jobs 
kubectl logs  pyspark-shell-02bc048a3dea5562-exec-1 -n spark-jobs 
kubectl logs  pyspark-shell-02bc048a3dea5562-exec-2 -n spark-jobs 
telnet mongodb-mongos.mongo-sharded.svc.cluster.local 27017
telnet mongodb-mongos.mongo-sharded 27017
telnet 147.96.81.119 27017
telnet 147.96.81.119 30159
kubectl get pods -n mongo-sharded
kubectl logs -n mongo-sharded mongodb-mongos-77cb5d8765-mdgqc
kubectl get pods -n mongo-sharded
kubectl get pods -n spak-jobs
kubectl get pods -n spak-job
kubectl get pods -n spark-jobs
kubectl get svc -n spark-jobs
./bin/docker-image-tool.sh -r estudianteuxm22 -t estudianteucm22/spark-mongo-monitor:latest -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t estudianteucm22/spark-mongo-monitor:latest -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t estudianteucm22/spark-mongo-monitor -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t spark-mongo-monitor -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t spark build
./bin/docker-image-tool.sh -r estudianteucm22 -t spark-mongo-monitor -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t spark build
docker push estudianteucm22/spark:spark-mongo-monitor
docker push eslocalhost/tudianteucm22/spark:spark-mongo-monitor
./bin/docker-image-tool.sh -r estudianteucm22 -t spark build
docker push eslocalhost/tudianteucm22/spark:spark-mongo-monitor
./bin/docker-image-tool.sh -r estudianteucm22 -t spark-mongo-monitor -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./bin/docker-image-tool.sh -r estudianteucm22 -t spark build
docker push localhost/tudianteucm22/spark:spark-mongo-monitor
docker push estudianteucm22/spark:spark-mongo-monitor
docker login
docker build -t estudianteucm22/jupyter_sparkmagic:latest .
docker login
docker build -t estudianteucm22/jupyter_sparkmagic:latest .
docker push estudianteucm22/jupyter_sparkmagic:latest
kubectl create configmap sparkmagic-config --from-file=sparmagicconfig.json=./sparkmagicconfig.json
kubectl create configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json
kubectl create configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json -n spark-jobs
kubectl edit  configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json -n spark-jobs
kubectl edit  configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json 
kubectl edit  configmap sparkmagic-config 
kubectl delete configmap sparkmagic-config 
kubectl delete configmap sparkmagic-config -n spark-jobs
kubectl create  configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json -n spark-jobs
cd ..
upgradestall jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
kubectl delete configmap sparkmagic-config -n spark-jobs
kubectl create  configmap sparkmagic-config --from-file=sparmagicconf.json=./sparkmagicconf.json -n spark-jobs
cd userpod/

helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd ..
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd userpod/
kubectl apply -f jupyter-pod-service.yaml 
cd ..
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd userpod/
kubectl apply -f jupyter-pod-service.yaml 
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd ..
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd userpod/
kubectl apply -f jupyter-pod-service.yaml 
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd ..
helm upgrade jupy-release jupyterhub/jupyterhub --namespace spark-jobs -f config.yaml
cd spark/
docker build -t estudianteucm22/pyspark-science:latest .
docker push estudi
docker push estudianteucm22/pyspark-science:latest
cd ..
kubectl get pvc -n mongo-sharded 
cd mongo
kubectl -f apply shard0-statefulset.yaml -n mongo-sharded
kubectl -f apply shard0-statefulset.yaml 
kubectl apply -n mongo-sharded -f shard0-statefulset.yaml 
kubectl get pvc -n mongo-sharded 
kubectl get storageclass rook-ceph-block -o=jsonpath='{.allowVolumeExpansion}'
kubectl edit pvc mongodb-shard0-data-mongodb-shard0-0 -n mongo-sharded
ls
cd jupyter/
ls
cat jupyther-pvc-claim.yaml 
cat config.yaml
ls
cd userpod
ls
cat jupyter-pod-service
cat jupyter-pod-service.yaml 
nano jupyter-pod-service.yaml 
cat jupyter-pod-service.yaml 
kubectl get pods -n monitoring 
kubectl get pods -n monitoring
kubectl get pods -n prometheus
ls
cat prometheus-config.yaml 
cd prometheus/
ls
cd config/
ls
cd testdata/
ls
cd ..
ls
cat prometheus_config.yaml 
cd scripts/
ls
cd ..
ls
cd ..
ls
cd metricsConfiguration/
ls
cat prometheus-config.yaml 
nano prometheus-config.yaml 
cat prometheus-config.yaml 
kubectl get services -n monitoring
kubectl get services -n mongo-sharded
ls
cat prometheus-deployment.yaml 
nano /etc/containers/registries.conf
kubectl get pods -n spark-jobs 
kubectl describe pod proxy-5bdcfbf6c4-ffb5w -n spark-jobs 
kubectl get svc -n spark-jobs 
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
cd spark-on-k8s/spark-3.4.1-bin-hadoop3/bin/
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.driver.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.driver.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
kubectl logs spark-pi-27e75e8a5aa6aa71-driver -n spark-jobs
kubectl get pvc -n spark-jobs
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=1  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
kubectl logs spark-pi-52c6fe8a5aacd37a-driver -n spark-jobs
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=1  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.driver.runAsUser=0  --conf spark.kubernetes.executor.runAsUser=0  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
kubectl logs spark-pi-1e7fed8a5ab41e1d-driver -n spark-jobs
./bin/spark-submit   --master k8s://https://kubernetes.default.svc:6443   --deploy-mode cluster   --name spark-pi   --class org.apache.spark.examples.SparkPi   --conf spark.executor.instances=2   --conf spark.kubernetes.namespace=spark-operator   --conf spark.kubernetes.container.image=docker.io/estudianteucm22/spark:spark-autorized   local:/path/to/spark/examples/jars/spark-examples_2.12-3.4.1.jar
cd spark-on-k8s
cd spark-3.4.1-bin-hadoop3/
ls
cd bin
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
kubectl get config
kubectl get config.io
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.driver.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events
spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.driver.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumeMounts.spark-eventlog-volume.mount.path=/mnt/events  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc
./spark-submit  --master k8s://https://master:6443  --deploy-mode cluster  --name spark-pi  --class org.apache.spark.examples.SparkPi  --conf spark.executor.instances=2  --conf spark.kubernetes.container.image=apache/spark-py:latest  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark  --conf spark.kubernetes.namespace=spark-jobs  --conf spark.eventLog.enabled=true  --conf spark.eventLog.dir=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.mount.path=/mnt/events  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-eventlog-volume.options.claimName=spark-eventlogs-pvc  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
ls
cd mongo
ls
cat configserver-statefulset.yaml 
cat configserver-service.yaml 
ls
cat mongos-deployment.yaml 
cat mongos-service.yaml 
ls
cat shard0-statefulset.yaml 
ls
cat shard0-service.yaml 
cat shard1-service.yaml 
cd mongodb-sharded/
ls
cd ..
ls
cd spark-on-k8s
ls
cd ..
cd spark-on-k8s
ls
cd
cd spark-on-k8s
cd spark-3.4.1-bin-hadoop3/
ls
cd kubernetes/
ls
cd ..
cd mongo
ls
nano kustomization.yaml
cat kustomization.yaml
cat comanditos.txt 
kubectl get services -n mongo-sharded
kubectl get pods -n mongo-sharded
cd ..
ls
cd monitoring/
ls
cd ..
cd metricsConfiguration/
ls
cat prometheus-config.yaml 
kubect get sparkApplications -n spark-jobs
kubectl get sparkApplications -n spark-jobs
kubectl delete sparkApplications spark-pi-scheduled-1693814419586659750 -n spark-jobs
kubectl get sparkApplications -n spark-jobs
kubectl delete -f spark-pi-schedule.yaml -n spark-jobs
cd metricsConfiguration/
ls
cat prometheus-service.yaml 
kubectl config view
docker -version
docker --version
docker info
docker ps
docker container ls
kubectl get pods -n spark-jobs
kubectl -n monitoring create token admin-user 
