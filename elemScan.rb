#!/bin/ruby

# Use bundler to keep dependencies local
require 'rubygems'
require 'bundler/setup'

###################################################################################################
# External code modules
require 'date'
require 'httparty'
require 'json'
require 'nokogiri'
require 'open3'
require 'pp'
require 'time'

###################################################################################################
# Use the right paths to everything, basing them on this script's directory.
def getRealPath(path) Pathname.new(path).realpath.to_s; end
$homeDir    = ENV['HOME'] or raise("No HOME in env")
$scriptDir  = getRealPath "#{__FILE__}/.."
$subiDir    = getRealPath "#{$scriptDir}/.."
$espylib    = getRealPath "#{$subiDir}/lib/espylib"
$erepDir    = getRealPath "#{$subiDir}/xtf-erep"
$arkDataDir = getRealPath "#{$erepDir}/data"
$controlDir = getRealPath "#{$erepDir}/control"
$jscholDir  = getRealPath "#{$homeDir}/eschol5/jschol"

# Special mode to print but don't execute changes.
$testMode = ARGV.delete('--test')
# Normal mode to execute changes
$goMode = ARGV.delete('--test')

# Internal libraries
require "#{$espylib}/xmlutil.rb"

# Go to the right server for the api
$escholServer = ENV['ESCHOL_FRONTEND_URL'] || raise("missing env ESCHOL_FRONTEND_URL")

$apiInfo = { host:     ENV['ELEMENTS_API_URL'] || raise("missing env ELEMENTS_API_URL"),
             username: ENV['ELEMENTS_API_USERNAME'] || raise("missing env ELEMENTS_API_USERNAME"),
             password: ENV['ELEMENTS_API_PASSWORD'] || raise("missing env ELEMENTS_API_PASSWORD") }

# Write out a spreadsheet of the results as we go along
$resultsSpreadsheet = nil

# Flush stdout after each write
STDOUT.sync = true

#################################################################################################
# Send a GraphQL query to the eschol access API, returning the JSON results.
#
# If variables are supplied, they should be like this: { var1: [type, value], var2: ... }
# where type is often "String"
#
def accessAPIQuery(query, vars = {})
  query = "query#{vars.empty? ? "" : "(#{vars.map{|name, pair| "$#{name}: #{pair[0]}"}.join(", ")})"} { #{query} }"
  resp = HTTParty.post("#{$escholServer}/graphql", :headers => { 'Content-Type' => 'application/json' },
                       :body => { query: query, variables: Hash[vars.map{|name,pair| [name.to_s, pair[1]]}] }.to_json)
  resp.code != 200 and raise("Internal error (graphql): " + "HTTP code #{resp.code} - #{resp.message}.\n#{resp.body}")
  resp['errors'] and raise("Internal error (graphql): #{resp['errors'][0]['message']}")
  return resp['data']
end

#################################################################################################
def parseFeedGrants(feedData)
  feedData.xpath(".//object[@category='grant']/records/record[@format='native']/native").map { |record|
    name = record.at("field[@name='funder-name']").text
    ref = record.at("field[@name='funder-reference']").text
    { name: name, ref: ref }
  }
end

#################################################################################################
def parseItemGrants(itemData)
  (itemData['grants'] || []).map { |grant|
    { name: grant }
  }
end

#################################################################################################
# Update the grant info in eScholarship for a publication.
def updateGrants(ark, grants)
  # Build a little UC-Ingest XML structure containing the new grant info
  uci = Nokogiri::XML("<uci:record xmlns:uci='http://www.cdlib.org/ucingest'/>")
  if !grants.empty?
    uci.root.find!('funding').build { |xml|
      grants.each { |grant|
        xml.grant(name: grant[:name], reference: grant[:ref])
      }
    }
  end

  # Use SubiGuts to replace the existing funding info for the item.
  cmd = "#{$subiDir}/lib/subiGuts.rb --replaceFunding #{ark.sub(%r{ark:/13030/}, '')} " +
        "'Funding updated on oapolicy.universityofcalifornia.edu' " +
        "'help@escholarship.org' -"
  Bundler.with_clean_env {  # super annoying that bundler by default overrides sub-bundlers Gemfiles
    out, err, status = Open3.capture3(cmd, stdin_data: uci.to_xml)
    status.exitstatus == 0 or raise("Error: subiGuts returned code #{status.exitstatus}. Stderr:\n#{err}")
  }
end

#################################################################################################
def extractWords(title)
  Set.new((title || "").gsub("&lt;", '<').gsub('&gt;', '>').gsub(/&\w+;/, '').
                        gsub(%r{</?\w+[^>]*>}, '').downcase.split(/\W+/))
end

#################################################################################################
# Scan for changes to one publication
def scanPub(unit, item)
  ark = item['id']
  pubID = item['localIDs'].select{ |pair| pair['scheme'] == 'OA_PUB_ID' }.dig(0, 'id')
  pubID or raise("can't find OA_PUB_ID in pub whose source is 'oa_harvester'")
  dateAdded = item['added']
  begin

    # Retrieve grant info from Elements
    resp = HTTParty.get("#{$apiInfo[:host]}/publications/#{pubID}/grants?detail=full",
             :basic_auth => { :username => $apiInfo[:username], :password => $apiInfo[:password] })
    if resp.code == 200
      data = Nokogiri::XML(resp.body).remove_namespaces!

      # On QA some of the pubs are radically different due to subsequent crawling. As a workaround,
      # if less than half the title words are the same, skip the pub.
      if $apiInfo[:host] =~ /qa/
        feedTitle = data.at("title").text.sub(/.*related to the publication: /, '')
        words1 = extractWords(feedTitle)
        words2 = extractWords(item['title'])
        if (words1 & words2).size <= ((words1.size + words2.size)/4)
          puts "  #{ark} (pub #{pubID}): skipping much-changed title: #{item['title'].inspect} vs #{feedTitle.inspect}"
          return
        end
      end

      # Parse it out and see if the grants are the same (if so we can skip)
      feedGrants = parseFeedGrants(data)
      itemGrants = parseItemGrants(item)
      feedSum = feedGrants.map{ |grant| grant[:name] }.sort.uniq.join("||")
      itemSum = itemGrants.map{ |grant| grant[:name] }.sort.uniq.join("||")
      if feedSum == itemSum
        #puts "#{ark}: funding matches."
        return
      end

      # We have new grant info. Update it in the item and add a spreadsheet line.
      $resultsSpreadsheet.print("#{unit}\t#{ark}\t#{dateAdded}\t#{pubID}\t")
      if itemSum.empty?
        puts "#{ark} (pub #{pubID}): funding added: #{feedSum.inspect}"
        $resultsSpreadsheet.puts("added\t#{feedSum.sub('"', "'").inspect}")
      elsif feedSum.empty?
        puts "#{ark} (pub #{pubID}): funding removed: #{itemSum.inspect}"
        $resultsSpreadsheet.puts("removed\t#{itemSum.sub('"', "'").inspect}")
      else
        puts "#{ark} (pub #{pubID}): funding changed from #{itemSum.inspect} to #{feedSum.inspect}."
        $resultsSpreadsheet.puts("changed\tfrom #{itemSum.sub('"', "'").inspect} to #{feedSum.sub('"', "'").inspect}")
      end
      $resultsSpreadsheet.flush
      if $testMode
        puts "  (not changing due to --test mode)"
      elsif $goMode
        updateGrants(ark, feedGrants)
        puts "  Updated."
      else
        puts "Error: either --test or --go must be specified."
        exit 1
      end
    end
  rescue
    puts "Error context: ark=#{ark} pubID=#{pubID}"
    raise
  end
end

#################################################################################################
# Scan every eSchol item that came from Elements
def scanAll
  query = %{
    unit(id: $unit) {
      items(tags:["source:oa_harvester"], include:[PUBLISHED,EMBARGOED], order:ADDED_ASC, more: $more) {
        total
        more
        nodes {
          id
          title
          added
          grants
          localIDs {
            scheme
            id
          }
        }
      }
    }
  }

  ['lbnl', 'rgpo'].each { |unit|
    more = nil
    nDone = 0
    loop do
      data = accessAPIQuery(query, { unit: ['ID!', unit], more: ['String', more] }).dig('unit', 'items')
      total ||= data['total']
      nDone == 0 and puts "Scanning #{total} pubs for #{unit}."
      data['nodes'].each { |item|
        scanPub(unit, item)
        nDone += 1
      }
      puts "Scanned #{nDone} of #{total} pubs for #{unit}."
      more = data['more']
      break if !more
    end
  }

  puts "All done."
end

#################################################################################################
# The main routine
File.open("#{$scriptDir}/results.csv", "w") { |io|
  io.puts "unit\titem\tdateAdded\tpub\taction\tfunding\n"
  $resultsSpreadsheet = io
  scanAll
}