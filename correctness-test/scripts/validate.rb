nodes = api.get "nodes"

results = nodes.keys.inject({}) do |res, node_name|
  node_data = api.get "nodes/#{node_name}"
  node_recipes = node_data.expand!.recipes.with_version_constraints_strings
  resolved_dependencies = begin 
                            api.post("environments/#{node_data.chef_environment}/cookbook_versions",
                                     :run_list => node_recipes)
                            "GOOD"
                          rescue
                            # todo: more sophisticated error handling
                            "ERROR"
                          end
  res[node_name] = resolved_dependencies
  res
end

puts "Total:   #{results.size}"
puts "Success: #{results.select{|k,v| v == "GOOD"}.size}"
puts "Failure: #{results.select{|k,v| v == "ERROR"}.size}"
