package com.atik.fileparsingwithspark.controller;

import com.atik.fileparsingwithspark.service.KafkaSender;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.json.simple.JSONObject;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/fileparsing/api/v1")
@Api(value = "infoSense", description = "Determining the user data based on user specific information")
public class ServiceController {

	private final KafkaSender kafkaSender;

	public ServiceController(KafkaSender kafkaSender) {
		this.kafkaSender = kafkaSender;
	}

	@ApiOperation(value = "Create Users collection from parsing a file", response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 200, message = "Success"),
			@ApiResponse(code = 404, message = "The resource you were trying to reach is not found"),
			@ApiResponse(code = 400, message = "Bad Request"),
			@ApiResponse(code = 600, message = "Invalid Parameters"),
			@ApiResponse(code = 500, message = "Internal Server Error")}
	)
	@RequestMapping(value = "/pathUrl", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
	public ResponseEntity<JSONObject> addPathUrl(@RequestParam("modelName") String modelName,
												 @RequestBody String path) throws ExecutionException, InterruptedException {

		System.out.println("[POST pathUrl] path :: " + path);

		kafkaSender.send(path +","+ modelName);
		return ResponseEntity.ok(new JSONObject() {
			{
				put("message", "Message sent to the Kafka 'path' Topic Successfully.");
			}
		});
	}

}