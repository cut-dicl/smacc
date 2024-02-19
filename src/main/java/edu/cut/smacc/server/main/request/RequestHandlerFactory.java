package edu.cut.smacc.server.main.request;

/**
 * A factory for creating SMACC server request handlers.
 */
public abstract class RequestHandlerFactory {

    public static RequestHandler createGetRequestHandler(ClientConnectionHandler connectionHandler) {
        return new GetRequestHandler(connectionHandler);
    }

    public static RequestHandler createPutRequestHandler(ClientConnectionHandler connectionHandler) {
        return new PutRequestHandler(connectionHandler);
    }

    public static RequestHandler createDeleteRequestHandler(ClientConnectionHandler connectionHandler) {
        return new DeleteRequestHandler(connectionHandler);
    }

    public static RequestHandler createDeleteCacheRequestHandler(ClientConnectionHandler connectionHandler) {
        return new DeleteCacheRequestHandler(connectionHandler);
    }

    public static RequestHandler createListCacheRequestHandler(ClientConnectionHandler connectionHandler) {
        return new ListCacheRequestHandler(connectionHandler);
    }

    public static RequestHandler createFileStatusRequestHandler(ClientConnectionHandler connectionHandler)  {
        return new FileStatusRequestHandler(connectionHandler);
    }

}
