package com.minedata.service.rticHistory;


import java.io.File;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;
import com.minedata.rtic.RticRestService;
import com.minedata.utils.RedisUtil;
import com.navinfo.mapspotter.foundation.io.IOUtil;

@Path("rticHistory")
@Service("rticRestService")
@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
@Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_PLAIN_UTF_8})
public class RticRestServiceImpl implements RticRestService {

    private final static Logger log = Logger.getLogger(RticRestServiceImpl.class);


    @GET
    @Path("save/{version}/{timeSeq}")
    @Override
    public String saveRtic2RedisByParams(@PathParam("version") String version,
            @PathParam("timeSeq") String timeSeq) {

        String input =
                RTICProperties.rtic_home + File.separator + version + File.separator + timeSeq
                        + ".lzo";
        String output =
                RTICProperties.rtic_home + File.separator + version + File.separator + timeSeq
                        + ".txt";
        File lzoUncompressFile =
                RticHistoryHandle.lzoUncompressFile(RTICProperties.lzop_path, input, output);
        RedisUtil redis = new RedisUtil();
        redis.open(IOUtil.makeRedisParam(RTICProperties.redis_host, RTICProperties.redis_port,
                "minemap", 2));
        HistoryRticForShow.save2Redis(lzoUncompressFile, redis, version, timeSeq);
        return "success";
    }



}
