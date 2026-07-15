local function decimal_mod(value,divisor)
  local remainder=0
  for index=1,#value do remainder=(remainder*10+tonumber(string.sub(value,index,index)))%divisor end
  return remainder
end

local function valid_metric_number(value)
  return type(value)=="number" and value==value and value>=0 and value<=9007199254740991
end

local function choose_candidate(candidates, round_robin, mode, now, max_age, min_memory, min_cpu, max_goroutines)
  if mode=="redis_round_robin" then return candidates[decimal_mod(round_robin,#candidates)+1] end
  if mode~="redis_resource_aware" then return nil end
  local best={}
  local best_memory=nil
  local best_cpu=nil
  local best_goroutines=nil
  local best_placements=nil
  for _,candidate in ipairs(candidates) do
    local metrics=candidate.node["Metrics"]
    if type(metrics)=="table" then
      local memory=metrics["MemoryAvailableBytes"]
      local cpu=metrics["CPUAvailableMilliCores"]
      local goroutines=metrics["Goroutines"]
      local updated=metrics["UpdatedAtUnixMilli"]
      if valid_metric_number(memory) and valid_metric_number(cpu) and valid_metric_number(goroutines) and valid_metric_number(updated) and updated>0 and updated<=now and now-updated<=max_age and memory>=min_memory and cpu>=min_cpu and (max_goroutines==0 or goroutines<=max_goroutines) then
        local memory_bucket=math.floor(memory/268435456)
        local cpu_bucket=math.floor(cpu/100)
        local goroutine_bucket=math.floor(goroutines/100)
        local placements=redis.call("ZCARD",candidate.node["PlacementNodeKey"])
        local better=best_memory==nil or memory_bucket>best_memory or
          (memory_bucket==best_memory and cpu_bucket>best_cpu) or
          (memory_bucket==best_memory and cpu_bucket==best_cpu and goroutine_bucket<best_goroutines) or
          (memory_bucket==best_memory and cpu_bucket==best_cpu and goroutine_bucket==best_goroutines and placements<best_placements)
        local tied=best_memory~=nil and memory_bucket==best_memory and cpu_bucket==best_cpu and goroutine_bucket==best_goroutines and placements==best_placements
        if better then
          best={candidate};best_memory=memory_bucket;best_cpu=cpu_bucket;best_goroutines=goroutine_bucket;best_placements=placements
        elseif tied then
          table.insert(best,candidate)
        end
      end
    end
  end
  if #best==0 then return nil end
  return best[decimal_mod(round_robin,#best)+1]
end
