package net.sinchii.simpara;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SputServlet extends HttpServlet {
  private static final long serialVersionUID = -2352312489046534654L;

  public void doGet(HttpServletRequest req, HttpServletResponse res)
    throws ServletException, IOException {

    String json = req.getParameter("j");
    PrintWriter pw = res.getWriter();
    pw.print(json);
    pw.print("\r\n");
  }
}
