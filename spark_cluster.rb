ssh_lib = File.join(ENV["HOME"], "/net-ssh/lib")

$LOAD_PATH.unshift(ssh_lib)

require 'net/ssh'
require 'ping'
require 'rexml/document'


VM_NUMBER = 14

Throttler_port = 20000

Files = {
  :cluster_nodes => File.join(ENV["HOME"], "/host_list"),
  :throttler_ports => File.join(ENV["HOME"], "/thrott_ports")
}

Usernames = {
  :root_usr => 'root',
  :usr_name => 'ioi600'
}

include REXML

def provision_vms
  vm_ids = []
  File.truncate("/home/ioi600/set_file", 0) unless File.zero?("/home/ioi600/set_file")
  f = File.open("/home/ioi600/set_file", 'w')
  VM_NUMBER.times do
    output = `onevm create /home/ioi600/spark_vm.template`
    vm_id = output.chomp!.split(':')[1].strip!
    vm_ids << vm_id
    f.puts(vm_id)
    puts "VM with id #{vm_id} created"
  end
  2.times do
    output = `onevm create /home/ioi600/spark2_vm.template`
    vm_id = output.chomp!.split(':')[1].strip!
    vm_ids << vm_id
    f.puts(vm_id)
    puts "VM with id #{vm_id} created"
  end
  f.close
  vm_num = VM_NUMBER + 2
  check_status_of_vms(vm_num)
  get_ip_address_for_vms(vm_ids)
end

def get_ip_address_for_vms(vms)
  ip_list = []
  unless vms.nil? || vms.empty?
    vms.each do |vm_id|
      result = `onevm show "#{vm_id}" | grep IP`
      ip = result.chomp.split('=')[1].sub(',', '').strip.chomp('"').reverse.chomp('"').reverse
      ip_list << ip
    end
  end
  ip_list
end

def configure_and_start_spark_cluster (vm_pool)
   set_passwordless_ssh (vm_pool)
   delete_files_and_folders (vm_pool)
   configure_spark (vm_pool)
   configure_spark_workers (vm_pool)
   vm_pool.each{|vm| start_bandwidth_throttler vm}
   edit_master_hosts_file (vm_pool)
   edit_slaves_hosts_file (vm_pool)
   configure_hadoop (vm_pool)
   format_hdfs (vm_pool.first)
   start_hdfs_and_yarn (vm_pool)
   start_spark (vm_pool.first)
   configure_hibench (vm_pool)
end

def delete_files_and_folders vm_pool
  vm_pool.each do |vm|
    cmd = <<-eos
    cd /home/ioi600
    rm -rf bin/
    rm examples.desktop
    rm -rf lib/
    rm -rf spark-perf/
    rm -rf Weasel/
    rm contextualization.log
    touch monitor_in.txt
    touch monitor_out.txt
    python bandwidth-throttler/monitor_bandwidth.py eth0 monitor_out.txt monitor_in.txt proc > ~/ssh.stdout 2>&1 & disown
    eos
    ssh = Net::SSH.start("#{vm}", Usernames[:root_usr])
    res = ssh.exec!(cmd)
    puts res
    ssh.close
   end
end

 def configure_spark vm_pool
   vm_pool.each do |vm|
     cmd = <<-eos
     cd spark-2.0.2/conf
     touch slaves
     cat slaves.template >> slaves
     echo #{vm} >> slaves
     eos
     ssh = Net::SSH.start("#{vm_pool.first}", Usernames[:usr_name])
     res = ssh.exec!(cmd)
     puts res
     ssh.close
   end
 end

def configure_spark_workers vm_pool
   vm_pool.each do |vm|
     cmd = <<-eos
     cd spark-2.0.2/conf
     touch spark-env.sh
     touch spark-defaults.conf
     cat spark-env.sh.template >> spark-env.sh
     cat spark-defaults.conf.template >> spark-defaults.conf
     echo SPARK_MASTER_HOST=#{vm_pool.first} >> spark-env.sh
     echo SPARK_WORKER_MEMORY=10g >> spark-env.sh
     echo SPARK_WORKER_CORES=4 >> spark-env.sh
     echo SPARK_WORKER_INSTANCES=2 >> spark-env.sh
     echo SPARK_LOCAL_DIRS=/mnt >> spark-env.sh
     echo spark.rpc.askTimeout               600s >> spark-defaults.conf
     echo spark.network.timeout              600s >> spark-defaults.conf
     eos
     ssh = Net::SSH.start("#{vm}", Usernames[:usr_name])
     res = ssh.exec!(cmd)
     ssh.close
   end
 end

def configure_hibench vm_pool
   vm_pool.each do |vm|
     cmd = <<-eos
     cd /mnt/HiBench/conf
     touch hadoop.conf
     echo hibench.hadoop.home  /home/ioi600/hadoop-2.7.3 >> hadoop.conf
     echo hibench.hadoop.executable  /home/ioi600/hadoop-2.7.3/bin/hadoop >> hadoop.conf
     echo hibench.hadoop.configure.dir  /home/ioi600/hadoop-2.7.3/etc/hadoop >> hadoop.conf
     echo hibench.hdfs.master   hdfs://#{vm_pool.first}:9000 >> hadoop.conf
     echo hibench.hadoop.release   apache >> hadoop.conf
     touch spark.conf
     echo hibench.spark.home      /home/ioi600/spark-2.0.2  >> spark.conf
     echo hibench.spark.master   spark://#{vm_pool.first}:7077  >> spark.conf
     echo hibench.spark.version   spark2.0 >> spark.conf
     echo spark.executor.memory   10g >> spark.conf
     echo spark.driver.memory     4g  >> spark.conf
     sed -i '3d' hibench.conf
     sed -i '3i\hibench.scale.profile                large' hibench.conf
     sed -i '6d' hibench.conf
     sed -i '6i\hibench.default.map.parallelism       64'   hibench.conf
     sed -i '9d' hibench.conf
     sed -i '9i\hibench.default.shuffle.parallelism   64'   hibench.conf
     sed -i '57, 58d' hibench.conf
     sed -i '57i\hibench.masters.hostnames     #{vm_pool.first}' hibench.conf
     sed -i '58i\hibench.slaves.hostnames      #{vm_pool[1..-1].join(' ')}' hibench.conf
     sed -i '32d' hibench.conf
     sed -i '32i\hibench.hdfs.data.dir         ${hibench.hdfs.master}/mnt/HiBenchData' hibench.conf
     eos
     ssh = Net::SSH.start("#{vm}", Usernames[:usr_name])
     ssh.exec!(cmd)
     ssh.close
  end
end

def format_hdfs master_ip
  cmd = <<-eos
  cd hadoop-2.7.3/bin
  ./hdfs namenode -format
  eos
  ssh = Net::SSH.start("#{master_ip}", Usernames[:usr_name])
  res = ssh.exec!(cmd)
  puts res
  ssh.close
end

def start_spark master_ip
  cmd = <<-eos
  cd spark-2.0.2/sbin
  ./start-all.sh
  eos
  ssh = Net::SSH.start("#{master_ip}", Usernames[:usr_name])
  res = ssh.exec!(cmd)
  puts res
  ssh.close
end

def configure_hadoop vm_pool
  source = "<configuration><property><name>fs.default.name</name><value>hdfs://#{vm_pool.first}:9000</value></property><property><name>hadoop.tmp.dir</name><value>/mnt</value></property></configuration>"
  source_2 = "<configuration><property><name>dfs.replication</name><value>2</value></property></configuration>"
  fs = ""
  fs_2 = ""
  doc_2 = Document.new(source_2)
  doc = Document.new(source)
  formatter = REXML::Formatters::Pretty.new
  formatter_2 = REXML::Formatters::Pretty.new
  formatter_2.compact = true
  formatter.compact = true
  formatter.write(doc, fs)
  formatter_2.write(doc_2, fs_2)
  vm_pool.each do |vm|
  cmd = <<-eos
  mkdir /mnt/hadoop-ioi600/dfs/name
  cd hadoop-2.7.3/etc/hadoop
  sed -i '19d' core-site.xml
  sed -i '$d' core-site.xml
  echo "#{fs}" | tee -a core-site.xml
  eos
  cmd_2 = <<-eos
  cd hadoop-2.7.3/etc/hadoop
  sed -i '19d' hdfs-site.xml
  sed -i '$d' hdfs-site.xml
  echo "#{fs_2}" | tee -a hdfs-site.xml
  eos
  ssh = Net::SSH.start("#{vm}", Usernames[:usr_name])
  res = ssh.exec!(cmd)
  res_1 = ssh.exec!(cmd_2)
  puts res
  puts res_1
  ssh.close
 end
end

def start_hdfs_and_yarn vm_pool
  prepare_slaves_file vm_pool.first
  vm_pool.each do |vm|
    cmd = <<-eos
    cd hadoop-2.7.3/etc/hadoop
    echo #{vm} >> slaves
    eos
    ssh = Net::SSH.start("#{vm_pool.first}", Usernames[:usr_name])
    ssh.exec!(cmd)
    ssh.close
  end
  lunch_hdfs_demons vm_pool.first
end

def lunch_hdfs_demons master_ip
  cmd = <<-eos
  cd hadoop-2.7.3/sbin
  ./start-dfs.sh
  ./start-yarn.sh
  eos
  ssh = Net::SSH.start("#{master_ip}", Usernames[:usr_name])
  ssh.exec!(cmd)
  ssh.close
end

def prepare_slaves_file master_ip
  cmd = <<-eos
  cd hadoop-2.7.3/etc/hadoop
  truncate -s 0 slaves
  eos
  ssh = Net::SSH.start("#{master_ip}", Usernames[:usr_name])
  ssh.exec!(cmd)
  ssh.close
end

def start_bandwidth_throttler vm
  vm_port = Throttler_port
  File.open(Files[:throttler_ports], 'a') do |f|
    f.puts "#{vm}\t#{vm_port}"
  end
  cmd = <<-eos
  touch ssh.stdout
  cd /home/ioi600/bandwidth-throttler
  python shape_traffic_server.py --port #{vm_port} > ~/ssh.stdout 2>&1 & disown
  eos
  ssh = Net::SSH.start("#{vm}", Usernames[:root_usr])
  ssh.exec!(cmd)
  ssh.close
  puts "bandwidth-throttler started on cluster host #{vm}"
end

def edit_master_hosts_file vm_pool
  cmd_1 = <<-eos
  touch /tmp/test.txt
  sed -i '1, 2d' /etc/hosts
  eos
  ssh = Net::SSH.start("#{vm_pool.first}", Usernames[:root_usr])
  ssh.exec!(cmd_1)
  ssh.close
  vm_pool[1..-1].each do |vm|
  cmd_2 = <<-eos
  cd /etc
  echo '#{vm}   Spark-vm' | cat - hosts > /tmp/test.txt && mv /tmp/test.txt hosts
  eos
  ssh = Net::SSH.start("#{vm_pool.first}", Usernames[:root_usr])
  ssh.exec!(cmd_2)
  ssh.close
 end
  cmd_3 = <<-eos
  cd /etc
  echo '#{vm_pool.first}    Spark-vm' | cat - hosts > /tmp/test.txt && mv /tmp/test.txt hosts
  eos
  ssh = Net::SSH.start("#{vm_pool.first}", Usernames[:root_usr])
  ssh.exec!(cmd_3)
  ssh.close
end

def edit_slaves_hosts_file vm_pool
  vm_pool[1..-1].each do |vm|
  cmd = <<-eos
  touch /tmp/test.txt
  sed -i '1, 2d' /etc/hosts
  eos
  ssh = Net::SSH.start("#{vm}", Usernames[:root_usr])
  ssh.exec!(cmd)
 end
  vm_pool[1..-1].each do |v|
    vm_pool[1..-1].each do |vm|
     unless vm == v
      cmd = <<-eos
      cd /etc
      echo '#{vm}   Spark-vm' | cat - hosts > /tmp/test.txt && mv /tmp/test.txt hosts
      eos
      ssh = Net::SSH.start("#{v}", Usernames[:root_usr])
      ssh.exec!(cmd)
      ssh.close
     end
    end
   end

  vm_pool[1..-1].each do |v|
   cmd = <<-eos
   cd /etc
   echo '#{v}   Spark-vm' | cat - hosts > /tmp/test.txt && mv /tmp/test.txt hosts
   eos
   ssh = Net::SSH.start("#{v}", Usernames[:root_usr])
   ssh.exec!(cmd)
   ssh.close
  end
   vm_pool[1..-1].each_with_index do |v, idx|
    cmd = <<-eos
   cd /etc
   echo '#{vm_pool.first}   Spark-vm' | cat - hosts > /tmp/test.txt && mv /tmp/test.txt hosts
   eos
   ssh = Net::SSH.start("#{v}",	Usernames[:root_usr])
   ssh.exec!(cmd)
   ssh.close
  end
end

def start
  vm_pool = provision_vms
  reset_environment vm_pool
  vm_reachable? vm_pool

  configure_and_start_spark_cluster vm_pool
  puts "spark cluster configured and started successfully"
  exit(0)
end

def check_status_of_vms vm_num
  loop do
    print "\rinitializing VMs........"
    vm_list = `onevm list`
    running_vms = vm_list.each_line.select{|l| l.include? "runn"}
    if running_vms.count == vm_num
      break
    end
  end
end

def vm_reachable? vm_pool
  loop do
    print "\rhecking reachability of VMs"
    reachable_vms = []
    vm_pool.each do |host|
      reachable_vms << Ping.pingecho("#{host}")
    end
    break if reachable_vms.all? { |e| e == true  }
  end
end

def set_passwordless_ssh vm_pool
  vm_pool.each do |vm|
    cmd = <<-eos
    cd .ssh
    sshpass -p '1234' ssh-copy-id ioi600@#{vm}
    eos
    ssh = Net::SSH.start "#{vm_pool.first}" , "#{Usernames[:usr_name]}"
    puts "#{ssh.exec!(cmd)}"
  end
end

def reset_environment vm_list
  File.truncate(Files[:cluster_nodes], 0) unless File.zero?(Files[:cluster_nodes])
  File.open(Files[:cluster_nodes], 'a') do |f|
    f.puts "#{vm_list.first}\tmaster and worker"
  end
  vm_list[1..-1].each do |ip|
    Thread.new do
      File.open(Files[:cluster_nodes], 'a'){|f| f.puts "#{ip}\tworker"}
    end.join
  end
  File.truncate(Files[:throttler_ports], 0) unless File.zero?(Files[:throttler_ports])
end

if __FILE__ == $PROGRAM_NAME
  start
end
