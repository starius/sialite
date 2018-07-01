local counter = 1

function setup(thread)
   thread:set("id", counter)
   counter = counter + 1
end

function init(args)
    addresses = io.open("/tmp/addresses.txt")
    filesize = addresses:seek("end")
    if filesize % 77 ~= 0 then
        error("bad file size")
    end
    n = filesize / 77
    math.randomseed(os.time() + id)
end

function request()
    r = math.random(0, n - 1)
    addresses:seek("set", r * 77)
    address = addresses:read(76)
    return wrk.format(nil, '/v1/address-history?address=' .. address)
end
