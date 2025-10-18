package com.pm.javadynamodb.api.exception;

import com.pm.javadynamodb.api.dto.ErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

// @ControllerAdvice makes this class a global exception handler for the entire application.
@ControllerAdvice
public class GlobalExceptionHandler {

    // this method will be called whenever a TableNotFoundException is thrown
    @ExceptionHandler(TableNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleTableNotFoundException(TableNotFoundException ex) {
        ErrorResponse error = new ErrorResponse(HttpStatus.NOT_FOUND.value(), ex.getMessage());
        return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
    }

    // method to handle other general exceptions
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException ex) {
        ErrorResponse error = new ErrorResponse(HttpStatus.BAD_REQUEST.value(), ex.getMessage());
        return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
    }
}
