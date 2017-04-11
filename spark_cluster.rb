ssh_lib = File.join(ENV["HOME"], "/net-ssh/lib")

$LOAD_PATH.unshift(ssh_lib)


require 'net/ssh'

def provision_vms template_file, vm_num
  vm_ids = []

  vm_num.to_i.times do
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
  pub_key = get_master_public_key vm_ips.first
  vm_ips.each {|vm| set_authorized_key(vm, pub_key)}
  start_spark vm_ips.first
end

def start_spark master_ip
  cmd = <<-eos
  cd /home/ioi600/spark-2.0.2/sbin
  ./start-all.sh
  eos
  ssh = Net::SSH.start("#{master_ip}", 'root')
  res = ssh.exec!(cmd)
  puts res
end

def start_bandwidth_throttler vm_ips
  port_nums = (200..250).map{|n| n}
  vm_ips.each do |ip|
    port = port_nums.shift
    cmd = <<-eos
    cd /home/ioi600/bandwidth-throttler
    python shape_traffic_server.py --port #{port}
    eos
    ssh = Net::SSH.start("#{ip}", 'root')
    res = ssh.exec!(cmd)
    puts res
  end

def start template, vms_list_file, vm_number
  ips = provision_vms template, vm_number
  File.truncate("#{vms_list_file}", 0) unless File.zero?("#{vms_list_file}")
  ips.each_with_index do |ip, idx|
    File.open("#{vms_list_file}", 'a') do |f|
      f.puts "#{ip}\tmaster" if idx == 0
      f.puts "#{ip}\tworker"
    end
  end
  configure_and_start_spark_cluster ips
  Thread.new {start_bandwidth_throttler(ips)}.join
end

  def get_master_public_key master_node
    ssh = Net.SSH.start("#{master_node}", "root")
    result = ssh.exec!("cat /root/.ssh/rsa_id.pub")
    return result
  end

  def set_authorized_key node pub_key
    ssh = Net::SSH.start "#{node}", "root"
    ssh.exec!("echo #{pub_key} >> /root/.ssh/authorized_keys")
  end

if __FILE__ == $PROGRAM_NAME
  vm_template = ARGV.shift
  vms_list_file = ARGV.shift
  vm_number = ARGV.shift
  start vm_template, vms_list_file, vm_number
else
  raise "Uable to run script"
end
