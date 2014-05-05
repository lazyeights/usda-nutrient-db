fs = require 'fs'

Stream = require 'stream'
mongodb = require 'mongodb'
async = require 'async'

{schema, filenames} = require '../data/sr26'

class ByLineStream extends Stream.Transform

  constructor: ->
    super objectMode: true

  _transform: (chunk, encoding, done) ->
    data = chunk.toString()
    if (@_lastLineData) then data = @_lastLineData + data
    lines = data.split('\r\n')
    @_lastLineData = lines.splice(lines.length-1,1)[0]
    lines.forEach @push.bind(@)
    done()

  _flush: (done) ->
    if (this._lastLineData) then @push @_lastLineData
    @_lastLineData = null
    done()

class UsdaToJSONStream extends Stream.Transform

  constructor: (@datafile) ->
    super objectMode: true

  _transform: (chunk, encoding, done) ->
    data = chunk.split('^').map (column) ->
      match = column.match(/~(.*)~/)
      if match then match[1]
      else if column then parseFloat column, 10
      else null

    record = {}
    record[column] = data[index] for column, index in schema[@datafile]

    @push record
    done()

class JSONStringifyStream extends Stream.Transform

  constructor: ->
    @start = true

    super
    @_writableState.objectMode = true
    @_readableState.objectMode = false

  _transform: (chunk, encoding, done) ->
    if @start
      @push '['
      @start = false
    else
      @push ','
    @push JSON.stringify chunk
    done()

  _flush: (done) ->
    @push ']'
    @start = false
    done()

class MongoDbImportStream extends Stream.Writable

  constructor: (@collection)->
    super objectMode: true

  _write: (chunk, encoding, done) ->
    @collection.insert chunk, (err, result) ->
      done()

datafile = 'weights'

processDatafile = (datafile, collection, cb) ->

  filename = filenames[datafile]

  fs.createReadStream './data/sr26/datafiles/'+filename
    .pipe new ByLineStream
    .pipe new UsdaToJSONStream datafile
    # .pipe new JSONStringifyStream
    # .pipe fs.createWriteStream './lib/'+filename+'.json'
    .pipe new MongoDbImportStream collection
    .on 'error', -> cb new Error
    .on 'finish', cb

importDatafile = (datafile, cb) ->

  mongodb.connect 'mongodb://localhost:27017/usda', (err, db) ->

    if err then throw new Error 'Cannot connect to MongoDB @ mongodb://localhost:27017/usda'

    async.waterfall [
      (cb) -> db.createCollection datafile, cb
      async.apply processDatafile, datafile
      (cb) -> db.close cb
    ], (err, results) -> 
      console.log "Imported collection #{datafile}" unless err
      cb(err)

module.exports = 
  
  clearDatabase: (cb) ->
    mongodb.connect 'mongodb://localhost:27017/usda', (err, db) ->
      if err then throw new Error 'Cannot connect to MongoDB @ mongodb://localhost:27017/usda'
      db.dropDatabase ->
        console.log 'Dropped existing MongoDB database \'usda\''
        db.close cb

  import: importDatafile

  datafiles: do -> (filename for filename of filenames)

  filenames: filenames

