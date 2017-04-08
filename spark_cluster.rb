ssh_lib = File.join(ENV["HOME"], "/net-ssh/lib")

$LOAD_PATH.unshift(ssh_lib)


require 'net/ssh'

def provision_vms template_file
  vm_ids = []
  number_of_vms = 16
  number_of_vms.times do
    output = `onevm create "#{template_file}"`
    vm_id = output.chomp!.split(':')[1].strip!
    vm_ids << vm_id
    puts "VM with id #{vm_id} created"
  end
  get_ip_address_for_vms vm_ids
end

def get_ip_address_for_vms vms
  ip_list = []
  unless vms.nil? || vms.empty?
    vms.each do |vm_id|
      result = `onevm show "#{vm_id}" | grep IP`
      ip = result.chomp.tr(',', '').split('=')[1]
      ip_list << ip
    end
  end
  ip_list
end

def configure_and_start_spark_cluster vm_ips
  vm_ips.each_with_index do |ip, idx|
    next if idx == 0
    configure_spark ip, vm_ips
  end
  start_spark vm_ips.first
end

 def configure_spark worker_ip, ip_list
  cmd = <<-eos
  cd /home/ioi600/spark-2.0.2/conf
  echo #{worker_ip} >> slaves.template
  cat /home/ioi600/.ssh/id_rsa.pub | ssh root@#{worker_ip} 'cat >> .ssh/authorized_keys'
  eos
  master_ip = ip_list.first
  ssh = Net::SSH.start("#{master_ip}", 'root')
  ssh.exec!(cmd)
end

def start_spark master_ip
  cmd = <<-eos
  cd /home/ioi600/spark-2.0.2/sbin
  ./start-master.sh
  ./start-slaves.sh
  eos
  ssh = Net::SSH.start("#{master_ip}", 'root')
  res = ssh.exec!(cmd)
  puts res
end

def start_bandwidth_throttler vm_ips
  port_nums = Array.new(16){rand(2000..20000)}
  vm_ips.each do |isp|
    port = port_nums.shift.to_s
    cmd = <<-eos
    python shape_traffic_server.py --port port
    eos
    ssh = Net::SSH.start("#{ip}", 'root')
    res = ssh.exec!(cmd)
    puts res
  end

def start template, vms_list_file
  ips = provision_vms template
  File.truncate("#{vms_list_file}", 0) unless File.zero?("#{vms_list_file}")
  File.open(vms_list_file, 'a') do |f|
    f.puts "Node\tRole"
    f.puts "#{ips.first}\tmaster"
  end
  ips.each_with_index do |ip, idx|
    cmd = <<-eos
    mkdir /home/ioi600/.ssh
    cd /home/ioi600/.ssh
    touch authorized_keys
    chmod 0600 authorized_keys
    eos
    next if idx == 0
    Thread.new {File.open("#{vms_list_file}", 'a') do |f|
      f.puts "#{ip}\worker"
    end}.join
    ssh = Net::SSH.start("#{ip}", 'root')
    ssh.exec!(cmd)
  end
  configure_and_start_spark_cluster ips
  start_bandwidth_throttler ips
end

if __FILE__ == $PROGRAM_NAME
  vm_template = ARGV.shift
  vms_list_file = ARGV.shift
  start vm_template, vms_list_file
else
  raise "Uable to run script"
end
