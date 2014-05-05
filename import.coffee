async = require 'async'

USDA = require './lib/usda'

USDA.clearDatabase ->

  async.each USDA.datafiles, USDA.import, (err) -> 
    if err then console.error err
    else
      console.log 'Import into MongoDB database \'usda\' complete.'