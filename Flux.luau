--[[
Created by stewhook.
Flux is a modular object pooling framework designed for simplicity and scalability.
Open-source and maintained at: https://github.com/stewhook/FluxPool
Report any issues to the github above. Feel free to contribute. Godspeed.

__/\\\\\\\\\\\\\\\__/\\\______________/\\\________/\\\__/\\\_______/\\\_        
 _\/\\\///////////__\/\\\_____________\/\\\_______\/\\\_\///\\\___/\\\/__       
  _\/\\\_____________\/\\\_____________\/\\\_______\/\\\___\///\\\\\\/____      
   _\/\\\\\\\\\\\_____\/\\\_____________\/\\\_______\/\\\_____\//\\\\______     
    _\/\\\///////______\/\\\_____________\/\\\_______\/\\\______\/\\\\______    
     _\/\\\_____________\/\\\_____________\/\\\_______\/\\\______/\\\\\\_____   
      _\/\\\_____________\/\\\_____________\//\\\______/\\\_____/\\\////\\\___  
       _\/\\\_____________\/\\\\\\\\\\\\\\\__\///\\\\\\\\\/____/\\\/___\///\\\_ 
        _\///______________\///////////////_____\/////////_____\///_______\///__
]]--

--!strict

local Pool = {}
Pool.__index = Pool

-- Generic helpers / types

export type InstanceList<I> = { I? } -- ring buffers will have nil holes
export type ConnectionMap<I> = { [I]: RBXScriptConnection }
export type TimeoutMap<I> = { [I]: number }
export type FactoryFn<I> = () -> I
export type ConstructFn<I, O> = (instance: I, opts: O?) -> ()
export type CleanupFn<I> = (instance: I) -> ()

export type QueueName = "hot" | "cold"

export type Queue<I> = {
	head: number,
	tail: number,
	count: number,
	buffer: InstanceList<I>,
}

export type IndexInfo<I> = {
	queue: QueueName,
	index: number,
}

export type IndexMap<I> = { [I]: IndexInfo<I> }

-- Pool type: I = instance type, O = options type
export type Pool<I, O> = {
	active: boolean,
	size: number,
	timeout: number,

	_hotSpot: Instance?,
	_coldSpot: Instance?,

	_hotQueue: Queue<I>,
	_coldQueue: Queue<I>,
	_indexMap: IndexMap<I>,

	-- Exposed mainly for debugging/introspection, necessary imo.
	hotStorage: InstanceList<I>,
	coldStorage: InstanceList<I>,

	connections: ConnectionMap<I>,
	activeTimeouts: TimeoutMap<I>,

	factoryFunction: FactoryFn<I>,

	-- Public API
	construct: (
		self: Pool<I, O>,
		constructFunction: ConstructFn<I, O>,
		constructOpts: O?
	) -> I?,
	store: (self: Pool<I, O>, part: I) -> (),
	wipe: (self: Pool<I, O>, cleanupFunction: CleanupFn<I>) -> (),
}

-- Private helpers

local function _getQueue<I, O>(self: Pool<I, O>, which: QueueName): Queue<I>
	if which == "hot" then
		return self._hotQueue
	else
		return self._coldQueue
	end
end

-- Total number of instances known to the pool
local function _getInstanceTotal<I, O>(self: Pool<I, O>): number
	return self._hotQueue.count + self._coldQueue.count
end

-- Decide whether or not to create an instance from scratch using the factoryFn.
local function _needConstruct<I, O>(self: Pool<I, O>, amount: number?): boolean
	amount = amount or 1
	local instanceCount = _getInstanceTotal(self)
	if instanceCount + (amount :: number) > self.size then
		return false
	end
	return true
end

-- Enqueue in O(1) into the chosen queue
local function _enqueue<I, O>(self: Pool<I, O>, which: QueueName, obj: I)
	local queue = _getQueue(self, which)

	if queue.count >= self.size then
		error("[FluxPool] Internal queue overflow; more instances than pool size.")
	end

	local tail = queue.tail + 1
	if tail > self.size then
		tail = 1
	end

	queue.tail = tail
	queue.buffer[tail] = obj
	queue.count += 1

	self._indexMap[obj] = {
		queue = which,
		index = tail,
	}
end

-- Skips nil holes created by removals.
local function _dequeue<I, O>(self: Pool<I, O>, which: QueueName): I?
	local queue = _getQueue(self, which)
	if queue.count == 0 then
		return nil
	end

	local size = self.size

	while queue.count > 0 do
		local head = queue.head
		local obj = queue.buffer[head]

		-- Advance head
		local nextHead = head + 1
		if nextHead > size then
			nextHead = 1
		end
		queue.head = nextHead

		if obj ~= nil then
			queue.buffer[head] = nil
			queue.count -= 1

			-- Clear index entry if present
			local info = self._indexMap[obj]
			if info ~= nil then
				self._indexMap[obj] = nil
			end

			return obj
		end
		-- if obj is nil, keep looping until one is found. If cout > 0 there is an instance somewhere.
	end

	return nil
end

local function _removeFromQueues<I, O>(self: Pool<I, O>, part: I): ()
	local info = self._indexMap[part]
	if info == nil then
		error("[FluxPool] There is no instance in any queue.")
		return
	end

	local queue = _getQueue(self, info.queue)
	local index = info.index
	local stored = queue.buffer[index]

	if stored ~= nil then
		queue.buffer[index] = nil
		queue.count -= 1
	end

	self._indexMap[part] = nil
end

-- Take the oldest instance, preferring cold over hot
local function _queueTake<I, O>(self: Pool<I, O>): I?
	local obj = _dequeue(self, "cold")
	if obj ~= nil then
		return obj
	end

	obj = _dequeue(self, "hot")
	if obj ~= nil then
		return obj
	end

	error("[FluxPool] There is no instance in any storage to take.")
end

local function _removeTimeout<I, O>(self: Pool<I, O>, part: I): ()
	if not self.activeTimeouts[part] then
		return
	end
	self.activeTimeouts[part] = nil
end

local function _startTimeout<I, O>(self: Pool<I, O>, part: I): ()
	if self.timeout <= 0 then
		return
	end -- Timeout of 0 equals no timeout.

	self.activeTimeouts[part] = DateTime.now().UnixTimestamp + self.timeout
end

local function _destroyListener<I, O>(self: Pool<I, O>, part: I): ()
	if self.connections[part] then
		return
	end

	local connection: RBXScriptConnection = (part :: any).Destroying:Connect(function()
		_removeTimeout(self, part)

		_removeFromQueues(self, part)

		local conn = self.connections[part]
		if conn ~= nil then
			conn:Disconnect()
			self.connections[part] = nil
		end
	end)

	self.connections[part] = connection
end

-- Public API

function Pool.new<I, O>(
	size: number, -- Max size of the pool.
	factoryFunction: FactoryFn<I>, -- The de-facto core function that creates the "template" of an object.
	timeout: number?, -- Optional timeout, auto stores objects after a set amount of time.
	hotStorage: Instance?, -- Optional instance, this is where active objects go for organization.
	coldStorage: Instance? -- Optional instance, this is where inactive objects go for organization.
): Pool<I, O>
	local self = setmetatable({}, Pool) :: Pool<I, O>

	timeout = timeout or 0
	if timeout < 0 then
		timeout = 0
	end	

	self.active = true -- Whether or not the object pool is active.
	self.size = size -- Maximum number of instances in the pool.
	self.timeout = timeout -- Timeout for auto storage.

	-- Backing buffers for our ring queues
	local hotBuffer = {} :: InstanceList<I>
	local coldBuffer = {} :: InstanceList<I>

	self._hotQueue = {
		head = 1,
		tail = 0,
		count = 0,
		buffer = hotBuffer,
	}

	self._coldQueue = {
		head = 1,
		tail = 0,
		count = 0,
		buffer = coldBuffer,
	}

	self._indexMap = {} :: IndexMap<I>

	-- Expose buffers for debugging/introspection
	self.hotStorage = hotBuffer
	self.coldStorage = coldBuffer

	self._hotSpot = hotStorage :: Instance?
	self._coldSpot = coldStorage :: Instance?
	self.connections = {} :: ConnectionMap<I>
	self.activeTimeouts = {} :: TimeoutMap<I>
	self.factoryFunction = factoryFunction

	-- Background timeout loop
	task.spawn(function()
		while self.active do
			local currentTime = DateTime.now().UnixTimestamp
			for object, timeoutTime in self.activeTimeouts do
				if currentTime < timeoutTime then
					continue
				end
				_removeTimeout(self, object)
				self:store(object)
			end
			task.wait(1)
		end
	end)

	return self
end

function Pool.construct<I, O>(
	self: Pool<I, O>,
	constructFunction: ConstructFn<I, O>,
	constructOpts: O?
): I?
	constructOpts = constructOpts or nil

	local object: I?

	-- Decide whether to construct a new instance or recycle from storage
	if _needConstruct(self) then
		object = self.factoryFunction()
	else
		object = _queueTake(self)
	end

	if constructOpts then
		constructFunction(object :: I, constructOpts)
	else
		constructFunction(object :: I, nil)
	end

	-- Mark as hot (in-use)
	_enqueue(self, "hot", object :: I)
	-- Places a .destroyed listener to handle cleanup if something happens.
	_destroyListener(self, object :: I)
	_startTimeout(self, object :: I)

	-- Parent to hotSpot if provided
	local inst = object :: Instance
	if inst.Parent ~= self._hotSpot then
		inst.Parent = self._hotSpot
	end
	
	return object
end

function Pool.store<I, O>(self: Pool<I, O>, part: I): ()
	_removeTimeout(self, part :: I)

	_removeFromQueues(self, part :: I)

	_enqueue(self, "cold", part :: I)

	local inst = part :: Instance
	inst.Parent = self._coldSpot
end

function Pool.wipe<I, O>(self: Pool<I, O>, cleanupFunction: CleanupFn<I>): ()
	-- Clean up all hot and cold instances, this does not stop the pool.
	
	local hotBuffer = self._hotQueue.buffer
	for i = 1, self.size do
		local obj = hotBuffer[i]
		if obj ~= nil then
			cleanupFunction(obj :: I)
		end
	end

	local coldBuffer = self._coldQueue.buffer
	for i = 1, self.size do
		local obj = coldBuffer[i]
		if obj ~= nil then
			cleanupFunction(obj :: I)
		end
	end
end

return Pool
