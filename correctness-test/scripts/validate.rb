# Failure data:
# | orgname | node_name | error_code / exception | error_message |

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

@errfile = File.new("errors.csv", "w")
@errfile.sync = true

# this assumes that the chef config is pointing at the root of
# the server api
#
@server_api = Chef::Config.chef_server_url

# TODO: think about taking a list of orgs as input.
# upon the first call to the couchdb orgs view, the chef
# api request times out (for all 5 retries)
#
puts "fetching orgs..."
orgs = api.get("organizations").keys

orgs[0..100].each do |org|
  # reset
  # - clear the cache
  # - reset the configured server url
  #   the chef-shell #api method returns a new api object every
  #   time you call it, configured with new Chef::Config values
  ROLE_CACHE.clear
  Chef::Config.chef_server_url = "#{@server_api}/organizations/#{org}"

  puts "resolviing #{org}..."
  nodes = api.get "nodes"

  results = nodes.keys.inject({}) do |res, node_name|
    node_data = api.get "nodes/#{node_name}"
    begin
      node_recipes = node_data.expand!.recipes.with_version_constraints_strings
    rescue ArgumentError => e
      @errfile.puts([org,
                     node_name,
                     "ArgumentError",
                     e.message].join(", "))
      res[node_name] = "ERROR"
      next res
    end
    resolved_dependencies = begin
                              # TODO: update chef to chef 11 or catch argument errors related to
                              # stupid recursive cookbooks
                              api.post("environments/#{node_data.chef_environment}/cookbook_versions",
                                       :run_list => node_recipes)
                              "GOOD"
                            rescue Net::HTTPServerException => e # Non 2XX/3XX
                              response = e.response
                              code     = response.code
                              message  = JSON.parse(e.response.body)['error']
                              @errfile.puts([org,
                                             node_name,
                                             code,
                                             message.to_s].join(", "))
                              "ERROR"
                            rescue Net::HTTPFatalError => e # 5XX
                              # TODO: client request tuning
                              # some orgs have no nodes that pass, so something is seriously
                              # wrong here and it's inefficient to wait over a minute per node
                              #
                              response = e.response
                              code     = response.code
                              message  = JSON.parse(e.response.body)['error']
                              @errfile.puts([org,
                                             node_name,
                                             code,
                                             message.to_s].join(", "))
                              "ERROR"
                            end
    res[node_name] = resolved_dependencies
    res
  end

  puts "#{org}: #{results.select{|k,v| v == "GOOD"}.size} / #{results.size}"
end

