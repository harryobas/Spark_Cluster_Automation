ssh_lib = File.join(ENV["HOME"], "/net-ssh/lib")

$LOAD_PATH.unshift(ssh_lib)

require 'net/ssh'
require 'ping'

VM_NUMBER = 4

Files = {
  :cluster_nodes => File.join(ENV["HOME"], "/host_list"),
  :throttler_ports => File.join(ENV["HOME"], "/thrott_ports")
}

Usernames = {
  :root_usr => 'root',
  :usr_name => 'ioi600'
}

def provision_vms
  vm_ids = []
  VM_NUMBER.times do
    output = `onevm create /home/ioi600/spark_vm.template`
    vm_id = output.chomp!.split(':')[1].strip!
    vm_ids << vm_id
    puts "VM with id #{vm_id} created"
  end
  check_status_of_vms VM_NUMBER
  get_ip_address_for_vms vm_ids
end

def get_ip_address_for_vms vms
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

def configure_and_start_spark_cluster vm_pool
  Thread.new do
    set_passwordless_ssh vm_pool
    configure_spark vm_pool
    start_spark vm_pool.first
    start_bandwidth_throttler vm_pool
  end.join
end

 def configure_spark vm_pool
   vm_pool.each do |vm|
     cmd = <<-eos
     cd spark-2.0.2/conf
     touch slaves
     cat slaves.template >> slaves
     echo #{vm} >> slaves
     eos
     ssh = Net::SSH.start("#{vm_pool.first}", "#{Usernames[:usr_name]}")
     res = ssh.exec!(cmd)
     puts res
   end
 end
def start_spark master_ip
  cmd = <<-eos
  cd spark-2.0.2/sbin
  ./start-all.sh
  eos
  ssh = Net::SSH.start("#{master_ip}", "#{Usernames[:usr_name]}")
  res = ssh.exec!(cmd)
  puts res
end

def start_bandwidth_throttler vm_pool
  port_nums = Array.new(16){rand(200)}.uniq
  vm_pool.each do |vm|
    vm_port = port_nums.shift.to_s
    File.open(Files[:throttler_ports], 'a') do |f|
      f.puts "#{vm}\t#{vm_port}"
    end
    cmd = <<-eos
    cd bandwidth-throttler
    python shape_traffic_server.py --port #{vm_port}
    eos
    ssh = Net::SSH.start("#{vm}", "#{Usernames[:root_usr]}")
    res = ssh.exec!(cmd)
    puts res
    puts "bandwidth-throttler server started on host #{vm}"
  end
end

def start
  vm_pool = provision_vms
  reset_environment vm_pool
  vm_reachable? vm_pool
    begin
      configure_and_start_spark_cluster vm_pool
    rescue StandardError => e
      puts "Error configuring and starting spark cluster"
      puts "#{e.message}"
    end
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
    print "\rchecking reachability of VMs"
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
    f.puts "#{ips.first}\tmaster and worker"
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
