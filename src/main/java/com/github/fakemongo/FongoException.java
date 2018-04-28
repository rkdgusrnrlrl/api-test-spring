package com.github.fakemongo;

/**
 * Internal fongo exception
 *
 * @author jon
 */
public class FongoException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Integer code;

  public FongoException(String message) {
    super(message);
    code = null;
  }

  public FongoException(Integer code, String message) {
    super(message);
    this.code = code;
  }

  public Integer getCode() {
    return code;
  }

}
