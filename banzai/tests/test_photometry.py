def do_photometrytestonFile(filename, reference_photzp, reference_colorterm, tmpdir):
    t = Table ([[filename],[-1],], names=['filename','frameid'])
    p = photcal.PhotCalib(os.path.expanduser('/Catalogs/refcat2/refcat2.db'))
    for r in t:
        photzp, photzpsig, colorterm = p.analyzeImage(r, useaws=False, outputimageRootDir=tmpdir)
        print (filename, photzp, photzpsig, colorterm)

    assert math.fabs (photzp - reference_photzp) < 0.1, f"Test for correct photomertic zeropoint of {filename}"
    assert math.fabs (colorterm - reference_colorterm) < 0.01, f"Test for correct colorterm of {filename}"

def test_photcalibration(tmpdir):

    print ("Data are stored in temp dir: ", tmpdir )
    startdir = os.path.dirname(os.path.abspath(__file__))

    do_photometrytestonFile(f"{startdir}/data/cpt1m012-fa06-20200113-0102-e91.fits.fz", 23.1,-0.008, tmpdir=tmpdir)
    do_photometrytestonFile(f"{startdir}/data/ogg2m001-ep04-20201006-0097-e91.fits.fz", 25.15, 0.063, tmpdir=tmpdir)

def test_match_catalog():
    pass

def test_zeropoint():
    pass