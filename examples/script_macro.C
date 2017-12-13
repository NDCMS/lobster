void
script_macro() {
	auto count = gApplication->Argc();
	gApplication->GetOptions(&count, gApplication->Argv());
	auto outname = gApplication->Argv(1);
	TFile out(outname, "RECREATE");
	TH1F hist("vertex_ndof", "", 500, -0.5, 499.5);
	TChain c("Events");
	for (int i = 2; i < count; ++i)
		c.Add(gApplication->Argv(i));
	c.Draw("recoVertexs_offlineSlimmedPrimaryVertices__PAT.obj.ndof_>>vertex_ndof");
	hist.Write();
}
