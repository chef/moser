# Failure data:
# | orgname | node_name | error_code | error_message |
FAILURES = []

ROLE_CACHE = {}

class Chef::RunList::RunListExpansionFromAPI
  def fetch_role(name, included_by)
    JSON.parse(ROLE_CACHE[name] ||= rest.get_rest("roles/#{name}").to_json)
  rescue Net::HTTPServerException => e
    if e.message == '404 "Not Found"'
      role_not_found(name, included_by)
    else
      raise
    end
  end
end

nodes = api.get "nodes"

results = nodes.keys.inject({}) do |res, node_name|
  node_data = api.get "nodes/#{node_name}"
  node_recipes = node_data.expand!.recipes.with_version_constraints_strings
  resolved_dependencies = begin 
                            api.post("environments/#{node_data.chef_environment}/cookbook_versions",
                                     :run_list => node_recipes)
                            "GOOD"
                          rescue Net::HTTPServerException => e
                            response = e.response
                            code     = response.code
                            message  = JSON.parse(e.response.body)['error']
                            FAILURES << ["att_services",
                                         node_name,
                                         code,
                                         message]
                            "ERROR"
                          end
  res[node_name] = resolved_dependencies
  res
end

puts "Total:   #{results.size}"
puts "Success: #{results.select{|k,v| v == "GOOD"}.size}"
puts "Failure: #{results.select{|k,v| v == "ERROR"}.size}"
require 'pp'
pp FAILURES
