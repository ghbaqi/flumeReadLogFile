local ngx = require 'ngx';
local ndk = require 'ndk';
local cjson_safe = require 'cjson.safe';

local _M = {}
local AUTH_CODE = "24v09Z0r6ESRIqi5wvCXAtnbuusKwn"

function make_response(status, respmsg)
    ngx.header["Content-Type"] = "application/json";
    ngx.status = status;
    if respmsg ~= nil then
        ngx.say(cjson_safe.encode(respmsg));
    end
    ngx.exit(status);
end

function verify(sign)
    local pos = string.find(sign, "_")
    if pos ~= nil then
        local ts = string.sub(sign, pos + 1)
        local str = AUTH_CODE .. "_hupu_upup_" .. ts
        local server_sign = ngx.md5(str) .. "_" .. ts;
        if server_sign == sign then
            return true;
        else
            return false;
        end
    else
        return false;
    end
end

function _M.handle()
    local method_name = ngx.req.get_method();
    local respmsg = {};
    --  判断请求方法
    if method_name == "GET" or method_name == "OPTION" then
        local get_args = ngx.req.get_uri_args();
        local auth_key = get_args["a_key"];
        if nil == auth_key then
            ngx.var.invalid = 1;
        else
            local check = verify(auth_key);
            if check == true then
                ngx.var.loggable = 1;
            else
                ngx.var.invalid = 1;
            end
        end
    else
        local headers = ngx.req.get_headers();
        -- auth_key判断
        local auth_key = headers["a_key"];
        if nil == auth_key then
            ngx.var.invalid = 1;
            respmsg.error_code = 50002;
            respmsg.error_msg = "不支持的请求";
            make_response(401, respmsg);
            return;
        else
            local check = verify(auth_key);
            if check == true then
                ngx.var.loggable = 1;
                ngx.exit(200);
            else
                ngx.var.invalid = 1;
                respmsg.error_code = 50001;
                respmsg.error_msg = "非法请求";
                make_response(401, respmsg);
                return;
            end
        end
    end
end

return _M
