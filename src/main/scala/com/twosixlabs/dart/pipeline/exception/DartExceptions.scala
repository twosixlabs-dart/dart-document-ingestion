package com.twosixlabs.dart.pipeline.exception

abstract class PersistenceException( val message : String, val operation : String, cause : Throwable ) extends Exception( message, cause )

class DartDatastoreException( message : String, operation : String, cause : Throwable = null ) extends PersistenceException( message, operation, cause )

class SearchIndexException( message : String, operation : String, cause : Throwable = null ) extends PersistenceException( message, operation, cause )

class ServiceIntegrationException( val message : String, val system : String, val cause : Throwable = null ) extends Exception( message, cause )

class UnrecognizedInputFormatException( val message : String, val cause : Throwable = null ) extends Exception( message, cause )

class AnnotatorFailedException( val message : String, val cause : Throwable ) extends Exception( message, cause )
