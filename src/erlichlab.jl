__precompile__()
module jlutils

using ConfParser
using MySQL
using DataFrames

function getDBConnection()
  conf = ConfParse(joinpath(homedir(),".dbconf"))
  parse_conf!(conf)
  user     = retrieve(conf, "client", "user")
  password = retrieve(conf, "client", "passwd");
  host     = retrieve(conf, "client", "host")
  dbc = mysql_connect(host, user, password)
end

function interesting_trials(pl::String, pp::DataArrays.NAtype)
  return false
end

function interesting_trials(pl::String, pp::String)
    poked = [strip(x,' ') for x in split(pp,',')]
    length(poked)>1 && (poked[end] == poked[end-1]) && contains(pl,poked[end])
end

function interesting_trials(inp::Tuple)
    interesting_trials(inp[0], inp[1])
end

"""
    interesting_trials(df::dataframe)
Input:
+ A dataframe (returned from mysql or readtable) generated as a query from proto.ff_view with at least poke_list and poked_pokes.

Output:
+ A list of trials where there were two of the same poke in the "poked_pokes" list and that poke was also in the "poke_list"

"""
function interesting_trials(inp::DataFrames.DataFrame)

    out = falses(size(inp,1))
    for dx = 1:size(inp,1)
      out[dx] = interesting_trials(inp[dx,:poke_list],inp[dx,:poked_pokes])
    end
    return out
end

"""
    double_poke_time(parsed_event::Dict)

Takes a dictionary (usually created with JSON.parse(beh.trials.data)) and finds 
the poke that generated a violation and returns the time from the previous poke of the same type to that poke.
"""
function double_poke_time(JD::Dict)
  viol_time = JD["vals"]["States"]["violationstate"][1]
  events = JD["vals"]["Events"]
  events_names = keys(events)
  db_time = NaN
  for k in events_names
    if k[end-1:end] == "in" && viol_time in events[k]
      viol_poke = k
      viol_ind = find(viol_time .== events[k])[1] # Find returns a vector...
      db_time = viol_ind == 1 ? NaN : events[k][viol_ind] - events[k][viol_ind-1]
      # If the violation poke was the first poke of this type then it was not a double poke
      # so return Nan, otherwise return the time between the double pokes.
      break
    end
  end
  return db_time
end

"""
    getAllPokes(parsed_event, sess_start=parsed_event["StartTime"])
Returns a tuple with 2 elements. The first is the sorted list of all the times
of the pokes in this trial. The 2nd is the poke_types that match the times. 
"""
function getAllPokes(pe::Dict; sess_start=pe["StartTime"])
   try
    evnames = [x for x in keys(pe["Events"])]
    in_ev = evnames[map(x->"in" == x[end-1:end], evnames)]
    poke_times = mapreduce(x->pe["Events"][x],vcat,in_ev) + pe["StartTime"]
    poke_types = mapreduce(x->fill(x,size(pe["Events"][x])),vcat,in_ev)
    si = sortperm(poke_times)
    poke_times = poke_times[si] - sess_start
    poke_types = poke_types[si]
    return poke_times, poke_types
  catch
    return [], []
  end
end

"""
    getAllPokes(parsed_event_history)
Returns a tuple with 2 elements. The first is the sorted list of all the times
of the pokes in this session. The 2nd is the poke_types that match the times. 
"""
function getAllPokes(peh::DataArrays.DataArray)
  start_time = peh[1]["StartTime"]
  poke_times = Float32[]
  poke_types = String[]
  for pe in peh
     out = getAllPokes(pe, sess_start=start_time)
     append!(poke_times, out[1])
     append!(poke_types, out[2])
   end
   return poke_times, poke_types
end

function countPokes(pe::Dict)
  # get the list of all the event names
  evnames = [x for x in keys(pe["Events"])]
  # then get only the ones that end in "in"
  in_ev = evnames[map(x->"in" == x[end-1:end], evnames)]
  # then count up all of the "in" events
  return sum(map(x->length(pe["Events"][x]),in_ev))
end

function slowGetAllPokes(peh::DataArrays.DataArray)
  # In matlab this would be faster! but in julia it is slower ....
    poke_counts = map(countPokes, peh)
    poke_times = Array{Float32}(sum(poke_counts))
    poke_types = Array{String}(sum(poke_counts))
    pind=1
    for px = 1:length(poke_counts)
      poke_times[pind:poke_counts[px]+pind-1], poke_types[pind:poke_counts[px]+pind-1] = getAllPokes(peh[px],sess_start=peh[1]["StartTime"])
      pind += poke_counts[px]
    end
    return poke_times, poke_types
end

export getAllPokes
export double_poke_time
export getDBConnection
export interesting_trials

end
