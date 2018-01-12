package com.minedata.exception;

public class ServiceExceptionBase extends BaseException {

	/**
	 * Constructors
	 * 
	 * @param code
	 *            错误代码
	 */
	public ServiceExceptionBase(String code) {
		super(code, null, code, null);
	}

	/**
	 * Constructors
	 * 
	 * @param cause
	 *            异常接口
	 * @param code
	 *            错误代码
	 */
	public ServiceExceptionBase(Throwable cause, String code) {
		super(code, cause, code, null);
	}

	/**
	 * Constructors
	 * 
	 * @param code
	 *            错误代码
	 * @param values
	 *            一组异常信息待定参数
	 */
	public ServiceExceptionBase(String code, Object[] values) {
		super(code, null, code, values);
	}

	/**
	 * Constructors
	 * 
	 * @param cause
	 *            异常接口
	 * @param code
	 *            错误代码
	 * @param values
	 *            一组异常信息待定参数
	 */
	public ServiceExceptionBase(Throwable cause, String code, Object[] values) {
		super(code, null, code, values);
	}

	private static final long serialVersionUID = -3711290613973933714L;

}
