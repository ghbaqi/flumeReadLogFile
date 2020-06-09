local allow_origin = "*"
ngx.header["Access-Control-Allow-Origin"] = allow_origin;
ngx.header["Access-Control-Allow-Headers"] = "Access-Control-Allow-Headers Authorization,Content-Type,Accept,Origin,User-Agent,DNT,Cache-Control,X-Mx-ReqToken,X-Data-Type,X-Requested-With,a_key";
if ngx.var.request_method == "OPTIONS" then
    ngx.header["Access-Control-Max-Age"] = "1728000";
    ngx.header["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS, PUT";
    ngx.header["Content-Length"] = "0";
    ngx.header["Content-Type"] = "application/json, charset=utf-8";
end