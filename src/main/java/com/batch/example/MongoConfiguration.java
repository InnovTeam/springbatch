package com.batch.example;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@ConfigurationProperties("migration.mongodb")
@Data
public class MongoConfiguration {
	private String uri;
	private String database;
	private String collection;

}
